package org.apache.jena.rdflink.connector;

import org.apache.jena.rdfconnection.Isolation;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.rdflink.dataset.DatasetGraphOverRDFLink;
import org.apache.jena.sparql.core.DatasetGraph;

public class DatasetGraphConnectorRDFLink
    implements DatasetGraphConnector
{
    @Override
    public RDFLink connnect(DatasetGraph dsg, Isolation isolation) {
        RDFLink result = null;
        if (dsg instanceof DatasetGraphOverRDFLink d) {
            result = d.newLink();
        }
        return result;
    }
}
