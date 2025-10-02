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
package org.apache.jena.sparql.service.enhancer.claimingcache;

import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.collect.Range;

/** Predicate to match by a range. */
public class PredicateRange<T extends Comparable<T>>
    implements Predicate<T>
{
    private Range<T> range;

    public PredicateRange(Range<T> range) {
        super();
        this.range = Objects.requireNonNull(range);
    }

    @Override
    public boolean test(T t) {
        boolean result = range.contains(t);
        return result;
    }

    @Override
    public String toString() {
        return range.toString();
    }
}
