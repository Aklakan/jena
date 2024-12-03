package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.serializer.SerializationContext;

public abstract class AbortableIterator1<I, O>
    extends AbortableIteratorBase<O>
{
    private AbortableIterator<I> input;

    public AbortableIterator1(AbortableIterator<I> input) {
        super();
        this.input = input;
    }

    protected AbortableIterator<I> getInput() { return input ; }

    @Override
    protected final void closeIterator() {
        closeSubIterator();
        performClose(input);
        input = null;
    }

    @Override
    protected final void requestCancel() {
        requestSubCancel();
        performRequestCancel(input);
    }

    /** Cancellation of the query execution is happening */
    protected abstract void requestSubCancel();

    /**
     * Pass on the close method - no need to close the QueryIterator passed to the
     * QueryIter1 constructor
     */
    protected abstract void closeSubIterator();

    // Do better
    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        // Linear form.
        if ( getInput() != null )
            // Closed
            getInput().output(out, sCxt);
        else
            out.println("Closed");
        out.ensureStartOfLine();
        details(out, sCxt);
        out.ensureStartOfLine();

//        details(out, sCxt) ;
//        out.ensureStartOfLine() ;
//        out.incIndent() ;
//        getInput().output(out, sCxt) ;
//        out.decIndent() ;
//        out.ensureStartOfLine() ;
    }

    protected void details(IndentedWriter out, SerializationContext sCxt) {
        out.println(Lib.className(this));
    }
}
