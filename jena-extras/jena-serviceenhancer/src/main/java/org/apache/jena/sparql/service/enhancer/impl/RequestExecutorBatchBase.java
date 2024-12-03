package org.apache.jena.sparql.service.enhancer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.jena.sparql.service.enhancer.impl.util.iterator.AbortableIterator;

interface Executor<I, O> {
    AbortableIterator<O> execute(I input);
}

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

