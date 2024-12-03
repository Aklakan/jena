package org.apache.jena.sparql.service.enhancer.impl.util.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

public class AbortableIterators {
    public static <T> AbortableIterator<T> empty() {
        return wrap(Collections.<T>emptyIterator());
    }

    public static AbortableIterator<Binding> adapt(QueryIterator qIter) {
        return new AbortableIteratorOverQueryIterator(qIter);
    }

    public static <T> AbortableIterator<T> wrap(Iterator<T> it) {
        return new AbortableIteratorOverIterator<>(it);
    }

    public static QueryIterator asQueryIterator(AbortableIterator<Binding> it) {
        return it instanceof AbortableIteratorOverQueryIterator x
            ? x.delegate()
            : new QueryIteratorOverAbortableIterator(it);
    }

    /** Wrap a given {@link QueryIterator} with an additional close action. */
    // XXX This static util method could be moved to QueryIter
    public static <T> AbortableIterator<T> onClose(AbortableIterator<T> qIter, Closeable action) {
        Objects.requireNonNull(qIter);
        AbortableIterator<T> result = action == null
            ? qIter
            : new AbortableIteratorWrapper<>(qIter) {
                @Override
                protected void closeIterator() {
                    try {
                        action.close();
                    } finally {
                        super.closeIterator();
                    }
                }
            };
        return result;
    }

    /** Wrap a given {@link QueryIterator} with an additional close action. */
    // XXX This static util method could be moved to QueryIter
//    private static QueryIterator onClose(QueryIterator qIter, Closeable action) {
//        Objects.requireNonNull(qIter);
//        QueryIterator result = action == null
//            ? qIter
//            : new QueryIteratorWrapper(qIter) {
//                @Override
//                protected void closeIterator() {
//                    try {
//                        action.close();
//                    } finally {
//                        super.closeIterator();
//                    }
//                }
//            };
//        return result;
//    }


//    public static map() {
//
//    }
//
//    public static flatMap() {
//
//    }
}
