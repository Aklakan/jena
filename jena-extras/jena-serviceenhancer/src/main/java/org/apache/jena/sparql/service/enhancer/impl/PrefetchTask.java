package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * A simple runnable which consumes items from an iterator upon calling {@link #run()}
 * and does so until {@link #stop()} is called or the thread is interrupted.
 */
public class PrefetchTask<T, I extends Iterator<T>>
    implements Runnable
{
    public enum State {
        CREATED,
        STARTING,
        RUNNING,
        TERMINATED
    }

    protected volatile I iterator;
    protected UnaryOperator<T> itemCopyFn;
    protected volatile List<T> bufferedItems;
    protected long maxBufferedItemsCount;

    protected volatile boolean isStopRequested;

    protected volatile State state = State.CREATED;

    public PrefetchTask(I iterator, long maxBufferedItemsCount, UnaryOperator<T> copyFn) {
        this(iterator, new ArrayList<>(1024), maxBufferedItemsCount, copyFn);
    }

    /**
     *
     * @param iterator
     * @param bufferedItems
     * @param maxBufferedItemsCount
     * @param copyFn A function to copy items before buffering them.
     *               Can be used to detach items from resources.
     *               The copyFn be null.
     */
    public PrefetchTask(I iterator, List<T> bufferedItems, long maxBufferedItemsCount, UnaryOperator<T> copyFn) {
        super();
        this.maxBufferedItemsCount = maxBufferedItemsCount;
        this.iterator = iterator;
        this.bufferedItems = bufferedItems;
        this.itemCopyFn = copyFn;
    }

    public List<T> getBufferedItems() {
        return bufferedItems;
    }

    public I getIterator() {
        return iterator;
    }

    public State getState() {
        return state;
    }

    public UnaryOperator<T> getCopyFn() {
        return itemCopyFn;
    }

    @Override
    public void run() {
        // Before the first item has been processed the state remainins in STARTING
        state = State.STARTING;
        while (!isStopRequested && !Thread.interrupted() && iterator.hasNext() && bufferedItems.size() < maxBufferedItemsCount) {
            state = State.RUNNING;
            T item = iterator.next();
            T copy = itemCopyFn == null ? item : itemCopyFn.apply(item);
            bufferedItems.add(copy);
        }
        state = State.TERMINATED;
    }

    public void stop() {
        isStopRequested = true;
    }

    public static <T, I extends Iterator<T>> PrefetchTask<T, I> of(I iterator, long maxBufferedItemsCount, UnaryOperator<T> copyFn) {
        return new PrefetchTask<>(iterator, maxBufferedItemsCount, copyFn);
    }
}
