/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl.util;

public class ExecutorServicePool {

}
//    private final ConcurrentHashMap<Integer, ExecutorService> executorMap = new ConcurrentHashMap<>();
//    private final AtomicInteger executorCounter = new AtomicInteger(0);
//    private final long idleTimeout;
//    private final TimeUnit timeUnit;
//
//    public static class ExecutorServiceWrapper
//        extends WrappingExecutorService {
//
//    }
//
//    public DynamicSingleThreadExecutorPool(long idleTimeout, TimeUnit timeUnit) {
//            this.idleTimeout = idleTimeout;
//            this.timeUnit = timeUnit;
//        }
//
//    // Request an executor (creates new if none is available)
//    public ExecutorService acquireExecutor() {
//        int key = executorCounter.incrementAndGet();
//        return executorMap.computeIfAbsent(key, k -> createSingleThreadExecutor(key));
//    }
//
//    // Releases the executor (allows custom behavior if needed)
//    public void releaseExecutor(ExecutorService executor, int key) {
//        // Optionally remove from the map immediately or manage via a timeout
//        executorMap.remove(key);
//        executor.shutdown();
//    }
//
//    // Shutdown all executors in the pool
//    public void shutdownAll() {
//        executorMap.forEach((key, executor) -> executor.shutdown());
//    }
//
//    private ExecutorService createSingleThreadExecutor(int index) {
//        ThreadFactory namedFactory = runnable -> {
//            Thread thread = new Thread(runnable);
//            thread.setName("single-thread-executor-" + index);
//            thread.setDaemon(true); // Daemon threads auto-shutdown with JVM
//            return thread;
//        };
//
//        ExecutorService executor = Executors.newSingleThreadExecutor(namedFactory);
//
//        // Use MoreExecutors to ensure the executor auto-shuts down after idleTimeout
//        return MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) executor, idleTimeout, timeUnit);
//    }
//}
