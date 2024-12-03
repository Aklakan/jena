package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import java.util.Iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.serializer.SerializationContext;

public class AbortableIteratorOverIterator<T, I extends Iterator<T>>
    extends AbortableIteratorBase<T>
{
    protected I iterator;

    public AbortableIteratorOverIterator(I iterator) {
        super();
        this.iterator = iterator;
    }

    I delegate() {
        return iterator;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.println(Lib.className(this) + "/" + Lib.className(iterator));
//        out.incIndent();
//        iterator.output(out, sCxt);
//        out.decIndent();
        // out.println(Utils.className(this)+"/"+Utils.className(iterator)) ;
    }

    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }

    @Override
    protected T moveToNextBinding() {
        return iterator.next();
    }

    @Override
    protected void closeIterator() {
    }

    @Override
    protected void requestCancel() {
    }
}
