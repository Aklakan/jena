package org.apache.jena.sparql.service.enhancer.claimingcache;

import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.collect.RangeSet;

/** Predicate to match by a range set. */
public class PredicateRangeSet<T extends Comparable<T>>
    implements Predicate<T>
{
    private RangeSet<T> rangeSet;

    public PredicateRangeSet(RangeSet<T> rangeSet) {
        super();
        this.rangeSet = Objects.requireNonNull(rangeSet);
    }

    @Override
    public boolean test(T t) {
        boolean result = rangeSet.contains(t);
        return result;
    }

    @Override
    public String toString() {
        return rangeSet.toString();
    }
}
