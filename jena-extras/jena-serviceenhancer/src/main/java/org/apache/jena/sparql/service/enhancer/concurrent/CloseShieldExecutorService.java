package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ForwardingExecutorService;

/** Wrapper for an executor service. Overrides the {@link #shutdown()} and {@link #shutdownNow()} with no-ops. */
public class CloseShieldExecutorService<X extends ExecutorService>
    extends ForwardingExecutorService {

    protected X delegate;
    protected AtomicBoolean isShutDown = new AtomicBoolean();

    public CloseShieldExecutorService(X delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    protected X delegate() {
        return delegate;
    }

    protected void checkOpen() {
        if (isShutdown()) {
            throw new RejectedExecutionException("Executor service is already shut down");
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        checkOpen();
        return super.submit(task);
    }

    @Override
    public Future<?> submit(Runnable task) {
        checkOpen();
        return super.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        checkOpen();
        return super.submit(task, result);
    }

    @Override
    public void shutdown() {
        isShutDown.set(true);
    }

    /** Immediately returns because only the view pretends to shut down. */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        isShutDown.set(true);
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return isShutDown.get() || super.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return super.isTerminated();
    }
}
