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

import java.util.Objects;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;

/** QueryIter backed by a QueryExec. The RowSet is materialized on the first call to next(). */
public class QueryIterMaterializeQueryExec
    extends QueryIter
{
    protected final QueryExec queryExec;
    protected RowSet baseRowSet;
    protected RowSet materializeRowSet;

    /**
     * Constructor.
     *
     * @param execCxt ExecutionContext. May be null, however cancellation signals are passed via contexts.
     * @param queryExec QueryIter. The backing query execution. Must not be null.
     * @param baseRowSet The backing row set. Must not be null.
     */
    public QueryIterMaterializeQueryExec(ExecutionContext execCxt, QueryExec queryExec, RowSet baseRowSet) {
        super(execCxt);
        this.queryExec = queryExec;
        this.baseRowSet = Objects.requireNonNull(baseRowSet);
    }

    @Override
    protected boolean hasNextBinding() {
        try {
            if (materializeRowSet == null) {
                return baseRowSet.hasNext();
            }
            return materializeRowSet.hasNext();
        } catch (HttpException ex) {
            throw QueryExceptionHTTP.rewrap(ex);
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        try {
            if (materializeRowSet == null) {
            	RowSet x = 
            	
            	// BUG - underling rows set does not have a cancel check!
                materializeRowSet = baseRowSet.materialize();
            }
            return materializeRowSet.next();
        } catch (HttpException ex) {
            throw QueryExceptionHTTP.rewrap(ex);
        }
    }

    @Override
    protected void closeIterator() {
        queryExec.close();
    }

    @Override
    protected void requestCancel() {
        queryExec.abort();
    }
}
