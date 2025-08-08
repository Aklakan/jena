package org.aksw.commons.tuple.rdfs;

import java.util.concurrent.Callable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/** Utils for guava caches. Methods do not declare exceptions and the cache may be null. */
public class CacheUtils {

    public static void invalidateAll(Cache<?, ?> cache) {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    public static <K, V> V getIfPresent(Cache<K, V> cache, K key) {
        V result = cache != null ? cache.getIfPresent(key) : null;
        return result;
    }

    public static <K, V> V get(Cache<K, V> cache, K key, Callable<? extends V> callable) {
        V result;
        if (cache == null) {
            try {
                result = callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            result = cache.get(key, k -> {
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return result;
    }

    public static CacheStats stats(Cache<?, ?> cache) {
        return cache == null
                ? CacheStats.empty() // CacheStats.of(0, 0, 0, 0, 0, 0, 0)
                : cache.stats();
    }

    /** Modify an existing builder to conditionally enable recording stats */
    public static <K, V> Caffeine<K, V> recordStats(Caffeine<K, V> builder, boolean onOrOff) {
        return onOrOff ? builder.recordStats() : builder;
    }
}
