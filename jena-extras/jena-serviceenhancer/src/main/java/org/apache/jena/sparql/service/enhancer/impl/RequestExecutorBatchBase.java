/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;

interface Executor<I, O> {
    AbortableIterator<O> execute(I input);
}

// FIXME Can probably be removed
abstract class BatchExecutorBase<G, I, O>
    implements Executor<GroupedBatch<G, Long, I>, O>
{
    @Override
    public AbortableIterator<O> execute(GroupedBatch<G, Long, I> batchRequest) {
        Batch<Long, I> batch = batchRequest.getBatch();
        NavigableMap<Long, I> batchItems = batch.getItems();

        G groupKey = batchRequest.getGroupKey();
        List<I> inputs = new ArrayList<>(batchItems.values());
        List<Long> reverseMap = new ArrayList<>(batchItems.keySet());

        AbortableIterator<O> result = execute(groupKey, inputs, reverseMap);
        return result;
    }

    protected abstract AbortableIterator<O> execute(G groupKey, List<I> inputs, List<Long> reverseMap);
}

