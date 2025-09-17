package org.apache.jena.rdflink.connector;

import org.apache.jena.rdfconnection.Isolation;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.rdflink.RDFLinkDataset;
import org.apache.jena.sparql.core.DatasetGraph;

public class DatasetGraphConnectorARQ
    implements DatasetGraphConnector
{
    @Override
    public RDFLink connnect(DatasetGraph dataset, Isolation isolation) {
//        RDFLinkDatasetBuilder builder = RDFLinkDatasetBuilder.newBuilder().dataset(dataset);
//        if (isolation != null) {
//            builder = builder.isolation(isolation);
//        }
//        return builder.build();
        return new RDFLinkDataset(dataset, isolation);
    }
}
