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

package org.apache.jena.dboe.storage.simple;

import java.util.function.Supplier;

import org.apache.jena.dboe.storage.DatabaseRDF;
import org.apache.jena.dboe.storage.StorageRDF;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCore2;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromMapOfTripleTableCore;
import org.apache.jena.dboe.storage.advanced.quad.QuadTableCoreFromSet;
import org.apache.jena.dboe.storage.advanced.quad.StorageRDFTriplesQuads;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore2;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromNestedMapsImpl;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableCoreFromSet;
import org.apache.jena.dboe.storage.system.DatasetGraphStorage;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.TransactionalLock;

/** Simple implementation of in-memory {@link DatabaseRDF} */
public class SimpleDB {
    public static DatasetGraph create() {
        return new DatasetGraphSimpleDB();
    }

    public static class DatasetGraphSimpleDB extends DatasetGraphStorage {
        public DatasetGraphSimpleDB() {
            super(new StorageMem(), new StoragePrefixesMem(), TransactionalLock.createMRSW());
        }
    }
    public static DatasetGraph createOrderPreserving() {
        return createOrderPreserving(false, false);
    }

    public static DatasetGraph createOrderPreserving(boolean strictOrderOnQuads, boolean strictOrderOnTriples) {
        Supplier<TripleTableCore> tripleTableSupplier = strictOrderOnTriples
                ? () -> new TripleTableCore2(new TripleTableCoreFromNestedMapsImpl(), new TripleTableCoreFromSet())
                : () -> new TripleTableCoreFromNestedMapsImpl();

        QuadTableCore quadTable = new QuadTableCoreFromMapOfTripleTableCore(tripleTableSupplier);

        if (strictOrderOnQuads) {
            quadTable = new QuadTableCore2(quadTable, new QuadTableCoreFromSet());
        }

        StorageRDF storage = StorageRDFTriplesQuads.createWithQuadsOnly(quadTable);
        DatasetGraph result = new DatasetGraphStorage(storage, new StoragePrefixesMem(), TransactionalLock.createMRSW());
        return result;
    }
}
