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

package org.apache.jena.sparql.service.enhancer.impl.util;

import java.util.NoSuchElementException;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;

/**
 * Abstract implementation of {@link QueryIter} that delegates computation of the next element to {@link QueryIterSlottedBase#moveToNext()}.
 */
public abstract class QueryIterSlottedBase
    extends QueryIter
{
    private boolean slotIsSet = false;
    private Binding slot = null;

    public QueryIterSlottedBase(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    protected boolean hasNextBinding() {
        if ( slotIsSet )
            return true;

//        boolean r = hasMore();
//        if ( !r ) {
//            close();
//            return false;
//        }

        slot = moveToNext();
        if ( slot == null ) {
            close();
            return false;
        }

        slotIsSet = true;
        return true;
    }

    @Override
    public Binding moveToNextBinding() {
        if ( !hasNext() )
            throw new NoSuchElementException(Lib.className(this));

        Binding obj = slot;
        slot = null;
        slotIsSet = false;
        return obj;
    }

    @Override
    protected void closeIterator() {
        // Called by QueryIterBase.close()
        slotIsSet = false;
        slot = null;
    }

    @Override
    protected void requestCancel() {
    }

    /**
     * Method that must return the next non-null element.
     * A return value of null indicates that the iterator's end has been reached.
     */
    protected abstract Binding moveToNext();

}
