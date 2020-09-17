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

package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 * @param <V>
 * @param <X>
 */
public abstract class StorageNodeForwarding<D, C, V, X extends StorageNode<D, C, V>>
    implements StorageNode<D, C, V>
{
    protected abstract X getDelegate();

    @Override
    public List<? extends StorageNode<D, C, ?>> getChildren() {
        return getDelegate().getChildren();
    }

    @Override
    public int[] getKeyTupleIdxs() {
        return getDelegate().getKeyTupleIdxs();
    }

    @Override
    public TupleAccessor<D, C> getTupleAccessor() {
        return getDelegate().getTupleAccessor();
    }

    @Override
    public <T> Streamer<V, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeysAsComponent(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeysAsTuples(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, ?> streamerForKeys(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeys(pattern, accessor);
    }

    @Override
    public C getKeyComponentRaw(Object key, int idx) {
        return getDelegate().getKeyComponentRaw(key, idx);
    }

    @Override
    public Object chooseSubStore(V store, int subStoreIdx) {
        return getDelegate().chooseSubStore(store, subStoreIdx);
    }

    @Override
    public <T> Streamer<V, ?> streamerForValues(T pattern, TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForValues(pattern, accessor);
    }

    @Override
    public <T> Streamer<V, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return getDelegate().streamerForKeyAndSubStoreAlts(pattern, accessor);
    }

    @Override
    public <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return getDelegate().streamEntries(store, tupleLike, tupleAccessor);
    }

}
