/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.tdb2.solver.index;

import java.util.List;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;

/**
 * A usable index together with the information needed to plan and execute a skip-scan.
 *
 * <p>{@link #order} is the sequence of projected variables in the order produced by the
 * index, i.e. following the index key/value slot order. Skip-scan results are emitted
 * ordered by this variable sequence, which the order-aware merge ({@link QueryIterMergeScanGroup})
 * relies on. Variables that participate in equality links are not part of the directly
 * observable order and are therefore omitted.</p>
 */
record SkipScanCandidate(
    TupleIndex index,
    IndexMatch match,
    List<Var> order) {
}
