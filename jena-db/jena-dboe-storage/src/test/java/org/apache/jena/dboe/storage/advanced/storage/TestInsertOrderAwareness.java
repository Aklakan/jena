/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */
package org.apache.jena.dboe.storage.advanced.storage;

import org.apache.jena.dboe.storage.advanced.core.DatasetGraphFactoryOrdered;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.Test;

public class TestInsertOrderAwareness {
    @Test
    public void test() {
        // FIXME TODO TO BE DONE
        Dataset ds = DatasetFactory.wrap(DatasetGraphFactoryOrdered.createTestDatasetGraph());
        RDFDataMgr.read(ds, "nato-phonetic-alphabet-example.trig");

        RDFDataMgr.write(System.out, ds, RDFFormat.TRIG_BLOCKS);
    }
}
