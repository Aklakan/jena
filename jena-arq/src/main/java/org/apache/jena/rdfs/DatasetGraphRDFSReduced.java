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

package org.apache.jena.rdfs;

import org.apache.jena.graph.Node;
import org.apache.jena.rdfs.engine.DatasetGraphWithGraphTransform;
import org.apache.jena.rdfs.engine.GraphMatchTransforms;
import org.apache.jena.rdfs.engine.Mappers;
import org.apache.jena.rdfs.setup.ConfigRDFS;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.Context;

/**
 * A DatasetGraph with an RDFS reasoning core aimed to improved over
 * {@link DatasetGraphRDFS}.
 */
public class DatasetGraphRDFSReduced
    extends DatasetGraphWithGraphTransform
{
    public DatasetGraphRDFSReduced(DatasetGraph dsg, ConfigRDFS<Node> setup) {
        super(dsg, GraphMatchTransforms.asGraphTransform(tf -> MatchRDFSReduced.create(setup, Mappers.mapperTriple(), tf)));
    }

    public DatasetGraphRDFSReduced(DatasetGraph dsg, Context cxt, ConfigRDFS<Node> setup) {
        super(dsg, cxt, GraphMatchTransforms.asGraphTransform(tf -> MatchRDFSReduced.create(setup, Mappers.mapperTriple(), tf)));
    }
}
