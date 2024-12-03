package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;

public class QueryIteratorOverAbortableIterator
    extends QueryIteratorBase
{
    private AbortableIterator<Binding> delegate;

    public QueryIteratorOverAbortableIterator(AbortableIterator<Binding> delegate) {
        super();
        this.delegate = delegate;
    }

    protected AbortableIterator<Binding> delegate() {
        return delegate;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        delegate().output(out, sCxt);
    }

    @Override
    protected boolean hasNextBinding() {
        return delegate().hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        return delegate().next();
    }

    @Override
    protected void closeIterator() {
        delegate().close();
    }

    @Override
    protected void requestCancel() {
        delegate().cancel();
    }
}
