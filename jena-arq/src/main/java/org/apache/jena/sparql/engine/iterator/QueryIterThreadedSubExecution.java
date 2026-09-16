/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.engine.iterator;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.jena.atlas.lib.Creator;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryException;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;

public class QueryIterThreadedSubExecution
	extends QueryIter
{
	private final Creator<? extends QueryExec> queryExecCreator;
	private final BlockingDeque<Elt> elts = new LinkedBlockingDeque<Elt>();
	private final Thread thread;
	private Binding peekedBinding = null;

	public QueryIterThreadedSubExecution(ExecutionContext execCxt, Creator<? extends QueryExec> queryExecCreator) {
		super(execCxt);
		this.queryExecCreator = queryExecCreator;
		this.thread = new Thread(this::run);
		thread.start();
	}

	private void ensurePeekedBinding() {
		Elt peekedElt;
		if (peekedBinding == null) {
			try {
				peekedElt = elts.take();
			} catch (InterruptedException | QueryCancelledException e) {
				throw new QueryCancelledException(e);
			}

			Throwable t = peekedElt.throwable;
			if (t != null) {
				if (t instanceof InterruptedException || t instanceof QueryCancelledException) {
					throw new QueryCancelledException(t);
				} else if (t instanceof HttpException e) {
		            throw QueryExceptionHTTP.rewrap(e);
				} else if (t instanceof QueryExceptionHTTP e) {
					throw e;
				} else {
					throw new QueryException(t);
				}
			}
			peekedBinding = peekedElt.binding;
		}
	}

	@Override
	protected boolean hasNextBinding() {
		ensurePeekedBinding();
		return peekedBinding != POISON;
	}

	@Override
	protected Binding moveToNextBinding() {
		ensurePeekedBinding();
		Binding result;
		if (peekedBinding == POISON) {
			throw new NoSuchElementException();
		}

		result = peekedBinding;
		peekedBinding = null;
		return result;
	}

	@Override
	protected void requestCancel() {
		thread.interrupt();
		elts.notifyAll();
	}

	@Override
	protected void closeIterator() {
		thread.interrupt();
		Duration timeout = Duration.ofSeconds(10);
		try {
			thread.join(timeout);
		} catch (InterruptedException e) {
			throw new ARQException("Abandoned thread which failed to terminate after " + timeout);
		}
	}

	// Producer thread logic.

	private record Elt(Binding binding, Throwable throwable) {}
	// private Elt POISON = new Elt(null, null);
	private Binding POISON = BindingFactory.binding(Var.alloc("__POISON___"), NodeFactory.createBlankNode());

	private void run() {
		ExecutionContext execCxt = QueryIterThreadedSubExecution.this.getExecContext();
		try (QueryExec queryExec = queryExecCreator.create()) {
			RowSet rs = queryExec.select();
			while (rs.hasNext()) {
				execCxt.checkCancelSignal();
				Binding binding = rs.next();
				elts.put(new Elt(binding, null));
			}
			forcePut(elts, new Elt(POISON, null));
		} catch (Exception e) {
			forcePut(elts, new Elt(null, e));
		}
	}

	private static <T> void forcePut(BlockingQueue<T> queue, T item) {
		retry: while (true) {
			try {
				queue.put(item);
				break;
			} catch (InterruptedException e) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e2) {
					// Ignore
				}
				continue retry;
			}
		}
	}
}
