package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 * FIXME The naming is horrible - it should be someting like StorageExpression or StorageFactory or
 * StorageManager or IndexNode or ...?
 * The 'Meta' in the name at present is because it is not the store itself but the factory for it
 * 'Node' because it is a node in a tree (or an expression)
 * '2' because its the second design
 *
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public interface Meta2Node<D, C, V> {

    /**
     * Each node in the storage expression may have 0 or more children
     *
     * @return
     */
    List<? extends Meta2Node<D, C, ?>> getChildren();

    /**
     * Future work; return a histogram for a corresponding store
     * Perhaps this method fits better on a derived interface such as IndexNodeMap
     *
     */
    // Histogram getHistogram(V store)

    /**
     * Should a node have a parent?
     * If we wanted to use subtrees of index nodes in different settings - especially
     * placing a bunch of them into a alternatives - then the parent differs from the context
     * Therefore the parent is left out
     *
     *
     */
//    Meta2Node<D, C, ?> getParent();


    /**
     * The component indexes by which this node indexes
     * May be empty but never null
     *
     * @return
     */
    int[] getKeyTupleIdxs();


    TupleAccessor<D, C> getTupleAccessor();

    /**
     * Create an object that can stream the content of the store
     *
     * @param <T>
     * @param tupleSupp
     * @param setter
     * @return
     */
//    <T> Stream<T> createStreamer(Supplier<T> tupleSupp, TupleSetter<T, C> setter);


    /**
     * Compiles from a given pattern a function that can stream the matching
     * keys from the appropriate store.
     * The keys must be instances of the component type otherwise an exception is raised
     *
     *
     * @return
     */
    <T> Streamer<V, C> streamerForKeysAsComponent(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    /**
     *
     * Compiles from a given pattern a function that can stream the matching
     * keys from the appropriate store.
     * The keys must be instances of the component type otherwise an exception is raised
     *
     * if getKeyTupleIdxs().length == 0 then returns a single tuple that projects no components
     *
     * @param store
     * @return
     */
    <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    <T> Streamer<V, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);


    C getKeyComponentRaw(Object key, int idx);


    <T> Streamer<V, ?> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);

    <T> Streamer<V, ? extends Entry<?, ?>> streamerForEntries(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);

    // <T> Streamer<V, Tuple<C>> streamerForE(T pattern, TupleAccessorCore<? super T, ? extends C> accessor);

    /**
     * Stream all entries under equality constraints obtained from a tuple-like pattern
     *
     * @param <T>
     * @param store
     * @param tupleLike
     * @param tupleAccessor
     * @return
     */
    <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor);

    @SuppressWarnings("unchecked")
    default <T> Stream<?> streamEntriesRaw(Object store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return streamEntries((V)store, tupleLike, tupleAccessor);
    }


    /**
     *
     *
     * @param <T>
     * @param store
     * @param tupleLike
     * @param tupleAccessor
     * @param path
     * @param currentIndex
     * @return
     */
//    <T> Stream<?> streamSurface(Object store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor,
//            List<Integer> path, int currentIndex);

    /**
     * Generic method to stream the content - mainly for debugging
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    default Stream<?> streamEntriesRaw(Object store) {
        // Stream with a null-tuple for which every component is reported as null (unconstrained)
        return streamEntries((V)store);
    }

    default Stream<?> streamEntries(V store) {
        // Stream with a null-tuple for which every component is reported as null (unconstrained)
        return streamEntriesRaw(store, null, (x, i) -> null);
    }

}
