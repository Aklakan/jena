package org.aksw.jenax.arq.util.tuple.adapter;

import java.util.Iterator;
import java.util.function.Function;

import org.aksw.commons.tuple.rdfs.DatasetGraphWrapperFindBase;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphWrapperView;
import org.apache.jena.sparql.core.Quad;

/**
 * A DatasetGraph wrapper where any find() request transforms the targeted graphs using
 * the provided lambda.
 */
public class DatasetGraphWithGraphTransform
    extends DatasetGraphWrapperFindBase
    implements DatasetGraphWrapperView {

    protected Function<Graph, Graph> graphWrapperFactory;

    public DatasetGraphWithGraphTransform(DatasetGraph dsg, Function<Graph, Graph> graphWrapperFactory) {
        super(dsg);
        this.graphWrapperFactory = graphWrapperFactory;
    }

    @Override
    protected Iterator<Quad> actionFind(boolean ng, Node g, Node s, Node p, Node o) {
        Iter<Node> graphIt = Node.ANY.equals(g)
                ? ng
                    ? Iter.iter(getR().listGraphNodes())
                    : Iter.concat(Iter.of(Quad.defaultGraphNodeGenerated), getR().listGraphNodes())
                : Iter.of(g);

       return graphIt.flatMap(gg -> {
           Graph graph = get().getGraph(gg);
           Graph wrappedGraph = graphWrapperFactory.apply(graph);
           Iterator<Triple> itTriples = wrappedGraph.find(s, p, o);
           Iterator<Quad> r = Iter.iter(itTriples).map(t -> Quad.create(gg, t));
           return r;
       });
    }
}
