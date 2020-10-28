/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.SetSupplier;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.Streamer;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.TupleValueFunction;
import org.apache.jena.ext.com.google.common.collect.Maps;

/**
 * Essentially a view of a Set<K> as a Map<K, Void>
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public class StorageNodeLeafComponentSet<D, C, V>
    extends StorageNodeSetBase<D, C, V>
{
    protected TupleValueFunction<C, V> valueFunction;

    // Reverse mapping of key to components
    protected TupleAccessorCore<? super V, ? extends C> keyToComponent;

//    public StorageNodeLeafComponentSet(
//            TupleAccessor<D, C> tupleAccessor,
//            SetSupplier setSupplier,
//            boolean holdsDomainTuples,
//            TupleValueFunction<C, V> valueFunction,
//            TupleAccessorCore<? super V, ? extends C> keyToComponent
//            ) {
//        super(new int[] {}, tupleAccessor, setSupplier);
//        this.valueFunction = valueFunction;
//        this.keyToComponent = keyToComponent;
//    }

    public StorageNodeLeafComponentSet(
            int tupleIdxs[],
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier,
            TupleValueFunction<C, V> valueFunction,
            TupleAccessorCore<? super V, ? extends C> keyToComponent
            ) {
        super(tupleIdxs, tupleAccessor, setSupplier);
        this.valueFunction = valueFunction;
        this.keyToComponent = keyToComponent;
    }

    @Override
    public boolean isSetNode() {
        return true;
    }

    @Override
    public Set<?> getStoreAsSet(Object store) {
        return (Set<?>)store;
    }


    public V tupleToValue(D tupleLike) {
        V result = valueFunction.map(tupleLike, (d, i) -> tupleAccessor.get(d, tupleIdxs[i]));
        return result;
    }


    @Override
    public List<StorageNode<D, C, ?>> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public boolean add(Set<V> set, D tupleLike) {
        V newValue = tupleToValue(tupleLike);

        // TODO We should use a separate type that explicitly allows null placeholders
        boolean result = set != null
                ? set.add(newValue)
                : false;

        return result;
    }

    @Override
    public boolean remove(Set<V> set, D tupleLike) {
        V newValue = tupleToValue(tupleLike);
        boolean result = set.remove(newValue);
        return result;
    }

    @Override
    public void clear(Set<V> store) {
        store.clear();
    }

    @Override
    public String toString() {
        return "(" + Arrays.toString(tupleIdxs) + ")";
    }

    @Override
    public <T> Streamer<Set<V>, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
//      return argSet -> argSet.stream();

        Streamer<Set<V>, V> baseStreamer = streamerForKeysUnderConstraints(pattern, accessor);
        // FIXME Ensure that the keys can be cast as components!
        return argSet -> baseStreamer.stream(argSet).map(key -> (C)key);
    }


    public <T> Streamer<Set<V>, V> streamerForKeysUnderConstraints(
            T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor)
    {
        Streamer<Set<V>, V> result;

        Object[] keyComponents = StorageNodeMapBase.projectTupleToArray(tupleIdxs, tupleLike, tupleAccessor);
        if (keyComponents != null) {
            @SuppressWarnings("unchecked")
            V key = valueFunction.map(keyComponents, (x, i) -> (C)x[i]);

            result = argSet -> argSet.contains(key)
                    ? Stream.of(key)
                    : Stream.empty();
        } else {
            result = argSet -> argSet.stream();
        }

        return result;
    }


    @Override
    public <T> Streamer<Set<V>, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return null;
    }

    @Override
    public <T> Streamer<Set<V>, V> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        throw new UnsupportedOperationException("There are no values to stream (Values can be seen as Tuple0 though)");
    }


    @Override
    public <T> Streamer<Set<V>, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(
//            int altIdx,
            T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        Streamer<Set<V>, Entry<?, ?>> result = streamerForKeysUnderConstraints(pattern, accessor)
                .mapItems(v -> Maps.immutableEntry(v, null));

        return result;
    }

    @Override
    public <T> Stream<V> streamEntries(Set<V> set, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        throw new UnsupportedOperationException("There are no entries to stream (Values can be seen as Tuple0 though)");
    }

    @Override
    public <T> Streamer<Set<V>, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return streamerForKeysUnderConstraints(pattern, accessor);
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        C result = keyToComponent.get((V)key, idx);
        return result;
    }

//    @Override
//    public Object chooseSubStore(Set<V> store, int subStoreIdx) {
//        throw new UnsupportedOperationException("leaf sets do not have a sub store");
//    }

    @Override
    public Object chooseSubStore(Set<V> store, int subStoreIdx) {
        if (subStoreIdx != 0) {
            throw new IndexOutOfBoundsException("Index must be 0 for inner maps");
        }

        // Return the store itself
        return store;
    }

}