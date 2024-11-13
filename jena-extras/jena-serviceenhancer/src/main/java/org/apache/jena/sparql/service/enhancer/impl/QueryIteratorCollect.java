/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * A QueryIterator that upon access of the first item consumes the underlying
 * iterator into a list.
 */
public class QueryIteratorCollect extends QueryIteratorWrapper {
    protected QueryIterator outputIt = null;
    protected ExecutionContext execCxt;

    protected List<Binding> cache = new ArrayList<>();

    public QueryIteratorCollect(QueryIterator qIter, ExecutionContext execCxt) {
        super(qIter);
        this.execCxt = execCxt;
    }

    @Override
    protected boolean hasNextBinding() {
        collect();
        return outputIt.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        collect();
        Binding b = outputIt.next();
        return b;
    }

    protected void collect() {
//        if (outputIt == null) {
//            synchronized (cache) {
        if (outputIt == null) {
            while (iterator.hasNext()) {
                Binding b = iterator.next();
                cache.add(b);
            }
            outputIt = QueryIterPlainWrapper.create(cache.iterator(), execCxt);
        }
//            }
//        }
    }

    @Override
    protected void closeIterator() {
        if (outputIt != null) {
            outputIt.close();
        }
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
    }
}
