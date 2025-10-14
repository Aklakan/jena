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

package org.apache.jena.rdflink.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.jena.rdfconnection.Isolation;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.dispatch.ParseCheckUtils;
import org.apache.jena.sparql.util.Context;

public class ConnectorRegistry {
    List<DatasetGraphConnector> dsgConnectors = Collections.synchronizedList(new ArrayList<>());

    // Singleton
    private static ConnectorRegistry registry;
    static { init(); }

    static public ConnectorRegistry get()
    {
        return registry;
    }

    public List<DatasetGraphConnector> getDsgConnectors() {
        return dsgConnectors;
    }

    /** If there is a registry in the context then return it otherwise yield the global instance */
    static public ConnectorRegistry chooseRegistry(Context context)
    {
        ConnectorRegistry result = get(context);
        if (result == null) {
            result = get();
        }
        return result;
    }

    /** Get the query engine registry from the context or null if there is none.
     *  Returns null if the context is null. */
    static public ConnectorRegistry get(Context context)
    {
        ConnectorRegistry result = context == null
                ? null
                : context.get(ARQConstants.registrySparqlDispatchers);
        return result;
    }

    static public void set(Context context, ParseCheckUtils registry)
    {
        context.set(ARQConstants.registrySparqlDispatchers, registry);
    }

    public ConnectorRegistry copy() {
        ConnectorRegistry result = new ConnectorRegistry();
        result.dsgConnectors.addAll(dsgConnectors);
        return result;
    }

    /** Create a copy of the registry from the context or return a new instance */
    public static ConnectorRegistry copyFrom(Context context) {
        ConnectorRegistry tmp = get(context);
        ConnectorRegistry result = tmp != null
                ? tmp.copy()
                : new ConnectorRegistry();
        return result;
    }

    public ConnectorRegistry() { }

    private static void init()
    {
        registry = new ConnectorRegistry();

        registry.add(new DatasetGraphConnectorARQ());
        registry.add(new DatasetGraphConnectorRDFLink());
    }

    // ----- Query -----

    /** Add a query dispatcher to the default registry */
    public static void addConnnector(DatasetGraphConnector f) { get().add(f); }

    /** Add a query dispatcher */
    public void add(DatasetGraphConnector f)
    {
        // Add to low end so that newer factories are tried first
        dsgConnectors.add(0, f);
    }

    /** Remove a query dispatcher */
    public static void removeConnector(DatasetGraphConnector f)  { get().remove(f); }

    /** Remove a query dispatcher */
    public void remove(DatasetGraphConnector f)  { dsgConnectors.remove(f); }

    /** Check whether a query dispatcher is already registered in the default registry */
    public static boolean containsFactory(DatasetGraphConnector f) { return get().contains(f); }

    /** Check whether a query dispatcher is already registered */
    public boolean contains(DatasetGraphConnector f) { return dsgConnectors.contains(f); }

    public static RDFLink connect(DatasetGraph dsg, Isolation isolation) {
        Objects.requireNonNull(dsg);

        Context cxt = dsg.getContext();
        ConnectorRegistry registry = chooseRegistry(cxt);

        RDFLink result = null;
        for (DatasetGraphConnector connector : registry.dsgConnectors) {
            result = connector.connnect(dsg, isolation);
            if (result != null) {
                break;
            }
        }
        Objects.requireNonNull(result, "No connector found for " + dsg.getClass());
        return result;
    }
}
