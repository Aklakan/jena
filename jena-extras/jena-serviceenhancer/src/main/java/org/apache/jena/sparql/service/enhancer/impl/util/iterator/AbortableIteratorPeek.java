package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import org.apache.jena.sparql.ARQInternalErrorException;

public class AbortableIteratorPeek<T>
    extends AbortableIterator1<T, T>
{
    private T binding = null ;
    private boolean closed = false ;

    public AbortableIteratorPeek(AbortableIterator<T> iterator) {
        super(iterator);
    }

    /** Returns the next binding without moving on.  Returns "null" for no such element. */
    public T peek()
    {
        if ( closed ) return null ;
        if ( ! hasNextBinding() )
            return null ;
        return binding ;
    }

    @Override
    protected boolean hasNextBinding()
    {
        if ( binding != null )
            return true ;
        if ( ! getInput().hasNext() )
            return false ;
        binding = getInput().nextBinding() ;
        return true ;
    }

    @Override
    protected T moveToNextBinding()
    {
        if ( ! hasNextBinding() )
            throw new ARQInternalErrorException("No next binding") ;
        T b = binding ;
        binding = null ;
        return b ;
    }

    @Override
    protected void closeSubIterator() {
        this.closed = true;
    }

    @Override
    protected void requestSubCancel() {
    }

//    @Override
//    public void output(IndentedWriter out, SerializationContext sCxt) {
//        // TODO Auto-generated method stub
//
//    }
}
