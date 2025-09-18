package org.apache.jena.sparql.exec.tracker;

import org.apache.jena.sparql.exec.QueryExec;

public interface QueryExecTransform {
    QueryExec postProcess(QueryExec queryExec);
}
