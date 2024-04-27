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

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.io.Printable;
import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.QueryOutputUtils;


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
        // super.closeI();
        slotIsSet = false;
        slot = null;
    }

    @Override
    protected void requestCancel() {
        System.err.println("CANCEL REQUESTED");
    }

    protected abstract Binding moveToNext();
}

/**
 * QueryIterator implementation based on IteratorSlotted.
 * Its purpose is to ease wrapping a non-QueryIterator as one based
 * on a {@link #moveToNext()} method analogous to guava's AbstractIterator.
 */
abstract class QueryIterSlottedBaseOld
    extends IteratorSlotted<Binding>
    implements QueryIterator
{
    protected boolean aborted;

    @Override
    public Binding nextBinding() {
        return next();
//        Binding result = isFinished()
//                ? null
//                : next();
//        return result;
    }

    @Override
    protected boolean hasMore() {
        return !aborted && !isFinished();
    }

    @Override
    public String toString(PrefixMapping pmap)
    { return QueryOutputUtils.toString(this, pmap); }

    // final stops it being overridden and missing the output() route.
    @Override
    public final String toString()
    { return Printable.toString(this); }

    /** Normally overridden for better information */
    @Override
    public void output(IndentedWriter out)
    {
        out.print(Plan.startMarker);
        out.print(Lib.className(this));
        out.print(Plan.finishMarker);
    }

    @Override
    public void cancel() {
        this.aborted = true;
    }

    public boolean isAborted() {
        return aborted;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        output(out);
//	        out.println(Lib.className(this) + "/" + Lib.className(iterator));
//	        out.incIndent();
//	        // iterator.output(out, sCxt);
//	        out.decIndent();
//	        // out.println(Utils.className(this)+"/"+Utils.className(iterator));
    }
}
