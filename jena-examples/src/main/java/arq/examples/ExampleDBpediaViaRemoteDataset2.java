package arq.examples;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdflink.RDFLinkHTTP;
import org.apache.jena.rdflink.dataset.DatasetGraphOverRDFLink;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.adapter.DsgSparqlExecutor;
import org.apache.jena.sparql.exec.adapter.DsgSparqlExecutorImpl;
import org.apache.jena.vocabulary.RDFS;

/**
 * Example that uses the {@link Resource} API over an HTTP-backed DatasetGraph.
 * Note, that each access fires an HTTP request. Hence, this pattern is only
 * meaningful lightweight infrequent lookups.
 * In general, it is recommended to first create local in-memory snapshots of
 * a remote endpoint's data using e.g. CONSTRUCT queries first.
 */
public class ExampleDBpediaViaRemoteDataset2 {
    public static void main(String... args) {
        Dataset ds = createDataset();

        resourceExample(ds);
        graphListExample(ds);
        modelQueryExample(ds);
    }

    public static void resourceExample(Dataset ds) {
        System.out.println("Resource Example: Accessing a SPARQL-backed endpoint via the Resource abstraction.");
        Model model = ds.getDefaultModel();
        Resource r = model.getResource("http://dbpedia.org/resource/Apache_Jena");
        List<RDFNode> list = Iter.toList(r.listProperties(RDFS.label).mapWith(Statement::getObject));
        int n = list.size();
        System.out.println("Got " + n + " labels from DBpedia:");
        IntStream.range(0, n).forEach(i -> {
            System.out.println("#" + (i + 1) + ": " + list.get(i));
        });
        System.out.println();

        /* Output is expected to be similar to:
             Got 5 labels from DBpedia:
             1: "Apache Jena"@en
             2: "Jena (Framework)"@de
             3: "Jena (framework)"@fr
             4: "Jena (informatica)"@it
                5: "아파치 제나"@ko
        */
    }

    public static void graphListExample(Dataset ds) {
        System.out.println("GraphList Example: Listing the graphs of a SPARQL-backed dataset.");
        Iterator<Resource> it = ds.listModelNames();
        List<Resource> graphs = Iter.toList(it); // Closes the iterator.
        int n = graphs.size();
        System.out.println("Got " + n + " (physical) graphs from DBpedia:");
        IntStream.range(0, n).forEach(i -> {
            System.out.println("#" + (i + 1) + ": " + graphs.get(i));
        });
        System.out.println();
    }

    public static void modelQueryExample(Dataset ds) {
        System.out.println("Model Query Example: Querying Model views over a SPARQL-backed dataset.");
        // Model namedModel = ds.getNamedModel("urn:dbpedia:live"); // This is the physical graph name.
        Model namedModel = ds.getNamedModel("http://dbpedia.org");
        try (QueryExecution qe = QueryExecution.model(namedModel).query("""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX bif: <http://www.openlinksw.com/schemas/bif#>
                SELECT * { ?s rdfs:label ?o . ?o bif:contains "Leipzig" } LIMIT 10
            """).build()) {
            ResultSetFormatter.out(qe.execSelect());
        }
    }

    public static Dataset createDataset() {
        // Virtuoso does not support the standard defalut query "SELECT ?g { GRAPH ?g { } }".
        // Solution: Override listGraphNodes.
        DsgSparqlExecutor virtExecutor = new DsgSparqlExecutorImpl() {
            @Override
            public IteratorCloseable<Node> listGraphNodes(Function<Query, ? extends QueryExec> executor,
                    PrefixMapping prefixes) {
                Var vg = Var.alloc("g");
                Query q = QueryFactory.create("SELECT DISTINCT ?g { GRAPH ?g { ?s ?p ?o } }");
                QueryExec qExec = executor.apply(q);
                return Iter.onClose(
                        Iter.map(qExec.select(), b -> b.get(vg)),
                        qExec::close);
            }
        };

        Dataset ds = DatasetFactory.wrap(DatasetGraphOverRDFLink.newBuilder()
            .linkCreator(() -> RDFLinkHTTP.newBuilder().destination("https://dbpedia.org/sparql").build())
            .executor(virtExecutor)
            .build());

        return ds;
    }
}
