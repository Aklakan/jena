package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * A query iterator that joins two or more iterators into a single iterator. */

public class AbortableIteratorConcat<T> extends AbortableIteratorBase<T>
{
    boolean initialized = false ;
    List<AbortableIterator<T>> iteratorList = new ArrayList<>() ;
    Iterator<AbortableIterator<T>> iterator ;
    AbortableIterator<T> currentQIter = null ;

    Binding binding ;
    boolean doneFirst = false ;

    public AbortableIteratorConcat()
    {
        super() ;
    }

    private void init()
    {
        if ( ! initialized )
        {
            currentQIter = null ;
            if ( iterator == null )
                iterator = iteratorList.listIterator() ;
            if ( iterator.hasNext() )
                currentQIter = iterator.next() ;
            initialized = true ;
        }
    }

    public void add(AbortableIterator<T> qIter)
    {
        if ( qIter != null )
            iteratorList.add(qIter) ;
    }


    @Override
    protected boolean hasNextBinding()
    {
        if ( isFinished() )
            return false ;

        init() ;
        if ( currentQIter == null )
            return false ;

        while ( ! currentQIter.hasNext() )
        {
            // End sub iterator
            //currentQIter.close() ;
            currentQIter = null ;
            if ( iterator.hasNext() )
                currentQIter = iterator.next() ;
            if ( currentQIter == null )
            {
                // No more.
                //close() ;
                return false ;
            }
        }

        return true ;
    }

    @Override
    protected T moveToNextBinding()
    {
        if ( ! hasNextBinding() )
            throw new NoSuchElementException(Lib.className(this)) ;
        if ( currentQIter == null )
            throw new NoSuchElementException(Lib.className(this)) ;

        T binding = currentQIter.nextBinding() ;
        return binding ;
    }


    @Override
    protected void closeIterator()
    {
        for ( AbortableIterator<T> qIter : iteratorList )
        {
            performClose( qIter );
        }
    }

    @Override
    protected void requestCancel()
    {
        for ( AbortableIterator<T> qIter : iteratorList )
        {
            performRequestCancel( qIter );
        }
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt)
    {
        out.println(Lib.className(this)) ;
        out.incIndent() ;
        for ( AbortableIterator<T> qIter : iteratorList )
        {
            qIter.output( out, sCxt );
        }
        out.decIndent() ;
        out.ensureStartOfLine() ;
    }
}
