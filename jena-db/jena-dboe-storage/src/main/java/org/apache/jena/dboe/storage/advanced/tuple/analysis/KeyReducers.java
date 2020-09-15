package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.jena.ext.com.google.common.collect.Maps;


public class KeyReducers {
    public static final KeyReducer<Entry<?, ?>> toPairs = (p, i, k) -> Maps.immutableEntry(p, k);


    /**
     * Simple reducer
     *
     *
     * @param <C>
     * @param idx
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <C> KeyReducer<C> projectOnly(int idx) {
        return (acc, i, obj) -> {
            // If a non-null value has been accumulated then pass it on
            // Otherwise check if the obj at index i match idx - if so return it as the accumulated value
            C r = acc != null
                    ? acc
                    : i == idx
                        ? (C)obj
                        : null;
            return r;
        };
    }



}
