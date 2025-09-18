package org.apache.jena.sparql.engine.connect;

import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.UpdateExecBuilder;

public interface ARQLink {
    QueryExecBuilder newQuery();
    UpdateExecBuilder newUpdate();
}
