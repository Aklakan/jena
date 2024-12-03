package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

public class AbortableIteratorOverQueryIterator
    extends AbortableIteratorBase<Binding>
{
    protected QueryIterator iterator;

    public AbortableIteratorOverQueryIterator(QueryIterator qIter) {
        iterator = qIter;
    }

    QueryIterator delegate() {
        return iterator;
    }

    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        return iterator.nextBinding();
    }

    @Override
    protected void closeIterator() {
        if ( iterator != null ) {
            iterator.close();
            iterator = null;
        }
    }

    @Override
    protected void requestCancel() {
        if ( iterator != null ) {
            iterator.cancel();
        }
    }

    @Override
    public void output(IndentedWriter out) {
        iterator.output(out);
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.println(Lib.className(this) + "/" + Lib.className(iterator));
        out.incIndent();
        iterator.output(out, sCxt);
        out.decIndent();
        // out.println(Utils.className(this)+"/"+Utils.className(iterator)) ;
    }
}
