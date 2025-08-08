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

package org.apache.jena.geosparql.query;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.geosparql.configuration.GeoSPARQLOperations;
import org.apache.jena.geosparql.spatial.SpatialIndexException;
import org.apache.jena.geosparql.spatial.index.v2.SpatialIndexLib;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdfs.DatasetGraphRDFS;
import org.apache.jena.rdfs.RDFSFactory;
import org.apache.jena.rdfs.SetupRDFS;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSetOps;

public class SpatialQueryTaskCurrent
    implements SpatialQueryTask
{
    private DatasetGraph baseDsg = null;
    private DatasetGraph effectiveDsg = null;
    private String query;

    @Override
    public void setData(String ttlString) throws Exception {
        baseDsg = RDFParser.create().fromString(ttlString).lang(Lang.TRIG).toDatasetGraph();
    }

    @Override
    public void setQuery(String queryString) throws Exception {
        this.query = queryString;
    }

    @Override
    public void setInferenceMode(boolean enableInferences, boolean materialize, int variant) {
        if (enableInferences) {
            Graph schemaGraph = GeoSPARQLOperations.loadGeoSPARQLSchema().getGraph();
            SetupRDFS setup = RDFSFactory.setupRDFS(schemaGraph);

            DatasetGraph virtualDsg = switch (variant) {
            case 0 -> RDFSFactory.datasetRDFS(baseDsg, setup); // Use the current default.
            case 1 -> new DatasetGraphRDFS(baseDsg, setup); // Specifically use DatasetGraphRDFS.
            default -> DatasetGraphFactory.empty();
            };

            if (true) {
                System.out.println("Number of quads in virtual dataset: " + Iter.count(virtualDsg.find()));
                System.out.println("Number of unique quads in virtual dataset: " + Iter.count(Iter.distinct(virtualDsg.find())));
            }

            if (materialize) {
                effectiveDsg = DatasetGraphFactory.create();
                effectiveDsg.addAll(virtualDsg);
            } else {
                effectiveDsg = virtualDsg;
            }
        } else {
            if (variant == 0) {
                effectiveDsg = baseDsg;
            } else {
                effectiveDsg = DatasetGraphFactory.empty();
            }
        }
    }

    @Override
    public void setIndex(boolean isEnabled) {
        if (isEnabled) {
            try {
                SpatialIndexLib.buildSpatialIndex(effectiveDsg);
            } catch (SpatialIndexException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public long exec() {
        try (QueryExec qe = QueryExec.dataset(effectiveDsg).query(query).build()) {
            long count = RowSetOps.count(qe.select());
            return count;
        }
    }
}
