package org.apache.jena.sparql.service.enhancer.impl.util;

public interface InstanceLifeCycle<T> {
    T newInstance();
    void closeInstance(T inst);
}
