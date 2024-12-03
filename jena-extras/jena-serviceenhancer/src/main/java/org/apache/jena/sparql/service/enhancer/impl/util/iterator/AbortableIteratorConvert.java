package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import java.util.function.Function;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.serializer.SerializationContext;

/** Iterator over another QueryIterator, applying a converter function
 *  to each object that is returned by .next() */
public class AbortableIteratorConvert<I, O> extends AbortableIterator1<I, O> {
    private Function<I, O> converter ;

    public AbortableIteratorConvert(AbortableIterator<I> iter, Function<I, O> c)
    {
        super(iter);
        this.converter = c ;
    }

    @Override
    public boolean hasNextBinding()
    {
        return getInput().hasNext() ;
    }

    @Override
    public O moveToNextBinding()
    {
        return converter.apply(getInput().next()) ;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.println(Lib.className(this)) ;
    }

    @Override
    protected void requestSubCancel() {
    }

    @Override
    protected void closeSubIterator() {
    }
}
