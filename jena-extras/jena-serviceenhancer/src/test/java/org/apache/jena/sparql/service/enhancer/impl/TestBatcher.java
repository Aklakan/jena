package org.apache.jena.sparql.service.enhancer.impl;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.ext.com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

public class TestBatcher {

    /** Test that grouping items correctly considers maxBatchSize and maxOutOfBindItemCount */
    @Test
    public void testBatcher() {
        int maxBatchSize = 3;
        int maxOutOfBandItemCount = 2;
        List<Entry<String, Integer>> input = List.<Entry<String, Integer>>of(
                Map.entry("a", 0),
                Map.entry("a", 1),
                Map.entry("b", 2),
                Map.entry("b", 3),
                Map.entry("b", 4),
                Map.entry("a", 5),
                Map.entry("c", 6),
                Map.entry("a", 7),
                Map.entry("c", 8),
                Map.entry("c", 9),
                Map.entry("d", 10),
                Map.entry("d", 11),
                Map.entry("c", 12),
                Map.entry("c", 13),
                Map.entry("d", 14),
                Map.entry("e", 15));

        // The numbers below refer to the value-component of the above entries
        List<List<Integer>> expectedBatchIds = List.<List<Integer>>of(
                List.of(0, 1),
                List.of(2, 3, 4),
                List.of(5, 7),
                List.of(6, 8, 9),
                List.of(10, 11, 14),
                List.of(12, 13),
                List.of(15)
        );

        IteratorCloseable<GroupedBatch<String, Long, Entry<String, Integer>>> it = new Batcher<String, Entry<String, Integer>>
            (Entry::getKey, maxBatchSize, maxOutOfBandItemCount).batch(Iter.iter(input.iterator()));
        // it.forEachRemaining(System.err::println);

        // For each obtained batch extract the list of values
        List<List<Integer>> actualBatchIds = Streams.stream(it)
                .map(groupedBatch -> groupedBatch.getBatch().getItems().values().stream().map(Entry::getValue).collect(Collectors.toList()))
                .collect(Collectors.toList());

        Assert.assertEquals(expectedBatchIds, actualBatchIds);
    }
}
