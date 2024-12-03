package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;

/** QueryIter-based wrapper which tracks open iterators in the execution context. */
public class QueryIterOverAbortableIterator
    extends QueryIter
{
    private AbortableIterator<Binding> delegate;

    public QueryIterOverAbortableIterator(ExecutionContext execCxt, AbortableIterator<Binding> delegate) {
        super(execCxt);
        this.delegate = delegate;
    }

    @Override
    protected boolean hasNextBinding() {
        return delegate.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        return delegate.next();
    }

    @Override
    protected void closeIterator() {
        delegate.close();
    }

    @Override
    protected void requestCancel() {
        delegate.cancel();
    }
}
