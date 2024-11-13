package org.apache.jena.sparql.service.enhancer.impl.util;

import java.util.Objects;
import java.util.function.Supplier;

/** Wrapper to initialize an instance lazily. */
public class Lazy<T> {
    protected Supplier<T> initializer;
    protected volatile T instance;

    public Lazy(Supplier<T> initializer, T instance) {
        super();
        this.initializer = initializer;
        this.instance = instance;
    }

    public static <T> Lazy<T> of(Supplier<T> initializer) {
        return new Lazy<>(Objects.requireNonNull(initializer), null);
    }

    public static <T> Lazy<T> of(T instance) {
        return new Lazy<>(null, Objects.requireNonNull(instance));
    }

    public T get() {
        if (instance == null) {
            synchronized (this) {
                if (instance == null) {
                    instance = initializer.get();
                }
            }
        }
        return instance;
    }
}
