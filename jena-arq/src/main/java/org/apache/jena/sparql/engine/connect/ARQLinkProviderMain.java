package org.apache.jena.sparql.engine.connect;

import org.apache.jena.sparql.core.DatasetGraph;

public class ARQLinkProviderMain
    implements ARQLinkProvider
{
    @Override
    public ARQLink connect(DatasetGraph dsg) {
        ARQLink link = ARQLinkProviderRegistry.connect(dsg);
        return link;
    }

//    @Override
//    public ARQLink connect(Graph graph) {
//        ARQLink link = ARQLinkProviderRegistry.connect(dsg);
//        return link;
//    }
}
