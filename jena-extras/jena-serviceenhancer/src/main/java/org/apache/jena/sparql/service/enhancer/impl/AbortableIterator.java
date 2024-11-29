package org.apache.jena.sparql.service.enhancer.impl;

import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.sparql.util.PrintSerializable;

public interface AbortableIterator<T> extends IteratorCloseable<T>, PrintSerializable
{
    /** Get next binding */
    public T nextBinding() ;

    /**
     * Cancels the query as soon as is possible for the given iterator
     */
    public void cancel();

    /*
     * Indicate whether this iterator is known to be an iterator of the join identity (one row, no columns).
     * Returns true if definitely a join identity; false for not or don't know.
     */
    public default boolean isJoinIdentity() { return false; }
}
