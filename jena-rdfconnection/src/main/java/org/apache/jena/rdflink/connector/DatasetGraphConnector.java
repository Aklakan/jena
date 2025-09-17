package org.apache.jena.rdflink.connector;

import org.apache.jena.rdfconnection.Isolation;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.core.DatasetGraph;

public interface DatasetGraphConnector {
    RDFLink connnect(DatasetGraph dsg, Isolation isolation);
}
