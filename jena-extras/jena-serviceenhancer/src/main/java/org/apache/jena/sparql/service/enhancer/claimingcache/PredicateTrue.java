package org.apache.jena.sparql.service.enhancer.claimingcache;

import java.util.function.Predicate;

public class PredicateTrue<T>
    implements Predicate<T>
{
    private static final Predicate<?> INSTANCE = new PredicateTrue<>();

    private PredicateTrue() {}

    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> get() {
        return (Predicate<T>) INSTANCE;
    }

    @Override
    public String toString() {
        return "TRUE";
    }

    @Override
    public boolean test(T t) {
        return true;
    }
}
