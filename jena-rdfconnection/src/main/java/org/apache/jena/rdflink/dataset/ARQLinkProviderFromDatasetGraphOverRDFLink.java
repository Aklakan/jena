package org.apache.jena.rdflink.dataset;

import org.apache.jena.atlas.lib.Creator;
import org.apache.jena.rdflink.RDFLink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.connect.ARQLink;
import org.apache.jena.sparql.engine.connect.ARQLinkProvider;

public class ARQLinkProviderFromDatasetGraphOverRDFLink
    implements ARQLinkProvider
{
    private class Adapter
        implements Creator<RDFLink>
    {
        private DatasetGraphOverRDFLink dsg;

        public Adapter(DatasetGraphOverRDFLink dsg) {
            super();
            this.dsg = dsg;
        }

        public DatasetGraph getDataset() {
            return dsg;
        }

        @Override
        public RDFLink create() {
            return dsg.newLink();
        }

    }

    @Override
    public ARQLink connect(DatasetGraph dsg) {
        ARQLink link = null;
        if (dsg instanceof DatasetGraphOverRDFLink d) {
            return new ARQLinkOverRDFLinkCreator(new Adapter(d));
        }
        return link;
    }
}
