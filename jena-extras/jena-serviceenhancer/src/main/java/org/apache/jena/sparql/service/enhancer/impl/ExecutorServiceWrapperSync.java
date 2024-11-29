package org.apache.jena.sparql.service.enhancer.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Utility wrapper for an ExecutorService to add synchronous API that abstracts away the Future. */
public class ExecutorServiceWrapperSync {
    protected ExecutorService executorService;

    public ExecutorServiceWrapperSync() {
        this(null);
    }

    public ExecutorServiceWrapperSync(ExecutorService es) {
        super();
        this.executorService = es;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void submit(Runnable runnable) {
        submit(() -> { runnable.run(); return null; });
    }

    public <T> T submit(Callable<T> callable) {
        if (executorService == null) {
            synchronized (this) {
                if (executorService == null) {
                    executorService = Executors.newSingleThreadExecutor();
                }
            }
        }
        T result = submit(executorService, callable);
        return result;
    }

    public static <T> T submit(ExecutorService executorService, Callable<T> callable) {
        try {
            return executorService.submit(callable).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
