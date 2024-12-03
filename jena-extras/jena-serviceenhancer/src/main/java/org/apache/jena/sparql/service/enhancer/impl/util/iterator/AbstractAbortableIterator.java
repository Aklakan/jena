package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jena.atlas.lib.Lib;

public abstract class AbstractAbortableIterator<T>
    extends AbortableIteratorBase<T>
{
    private boolean slotIsSet = false;
    // private boolean hasMore = true;
    private T slot = null;

    public AbstractAbortableIterator() {
        super();
    }

    public AbstractAbortableIterator(AtomicBoolean cancelSignal) {
        super(cancelSignal);
    }

    @Override
    protected boolean hasNextBinding() {
        if ( slotIsSet )
            return true;

    //    boolean r = hasMore();
    //    if ( !r ) {
    //        close();
    //        return false;
    //    }

        slot = moveToNext();
        if ( slot == null ) {
            close();
            return false;
        }

        slotIsSet = true;
        return true;
    }

    @Override
    public T moveToNextBinding() {
        if ( !hasNext() )
            throw new NoSuchElementException(Lib.className(this));

        T obj = slot;
        slot = null;
        slotIsSet = false;
        return obj;
    }

    @Override
    protected void closeIterator() {
        // Called by QueryIterBase.close()
        slotIsSet = false;
        slot = null;
    }

    @Override
    protected void requestCancel() {
    }

    /**
     * Method that must return the next non-null element.
     * A return value of null indicates that the iterator's end has been reached.
     */
    protected abstract T moveToNext();
}
