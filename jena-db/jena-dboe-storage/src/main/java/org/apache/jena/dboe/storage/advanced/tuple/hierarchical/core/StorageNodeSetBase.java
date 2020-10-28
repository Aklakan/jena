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

import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util.SetSupplier;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
abstract class StorageNodeSetBase<D, C, V>
    extends StorageNodeBase<D, C, Set<V>>
    implements StorageNodeMutable<D, C, Set<V>>
{
    protected SetSupplier setSupplier;

    public StorageNodeSetBase(
            int[] tupleIdxs,
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier
        ) {
        super(tupleIdxs, tupleAccessor);
        this.setSupplier = setSupplier;
    }

    @Override
    public Set<V> newStore() {
        return setSupplier.get();
    }

    @Override
    public boolean isEmpty(Set<V> store) {
        boolean result = store.isEmpty();
        return result;
    }
}