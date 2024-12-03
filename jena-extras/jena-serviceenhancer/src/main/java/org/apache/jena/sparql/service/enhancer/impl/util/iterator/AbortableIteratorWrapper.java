package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.serializer.SerializationContext;

public class AbortableIteratorWrapper<T>
    extends AbortableIteratorBase<T>
{
    protected AbortableIterator<T> iterator;

    public AbortableIteratorWrapper(AbortableIterator<T> qIter) {
        iterator = qIter;
    }

    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }

    @Override
    protected T moveToNextBinding() {
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
