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
