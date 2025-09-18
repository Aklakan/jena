package org.apache.jena.sparql.engine.connect;

import org.apache.jena.sparql.core.DatasetGraph;

public interface ARQLinkProvider {
    ARQLink connect(DatasetGraph dsg);
    // ARQLink connect(Graph graph);
}
