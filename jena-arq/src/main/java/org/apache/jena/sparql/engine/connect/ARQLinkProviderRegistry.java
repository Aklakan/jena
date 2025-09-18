package org.apache.jena.sparql.engine.connect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.dispatch.SparqlDispatcherRegistry;
import org.apache.jena.sparql.util.Context;

public class ARQLinkProviderRegistry {
    List<ARQLinkProvider> linkProviders = Collections.synchronizedList(new ArrayList<>());

    // Singleton
    private static ARQLinkProviderRegistry registry;
    static { init(); }

    static public ARQLinkProviderRegistry get()
    {
        return registry;
    }

    public List<ARQLinkProvider> getProviders() {
        return linkProviders;
    }

    /** If there is a registry in the context then return it otherwise yield the global instance */
    static public ARQLinkProviderRegistry chooseRegistry(Context context)
    {
        ARQLinkProviderRegistry result = get(context);
        if (result == null) {
            result = get();
        }
        return result;
    }

    /** Get the query engine registry from the context or null if there is none.
     *  Returns null if the context is null. */
    static public ARQLinkProviderRegistry get(Context context)
    {
        ARQLinkProviderRegistry result = context == null
                ? null
                : context.get(ARQConstants.registrySparqlDispatchers);
        return result;
    }

    static public void set(Context context, SparqlDispatcherRegistry registry)
    {
        context.set(ARQConstants.registrySparqlDispatchers, registry);
    }

    public ARQLinkProviderRegistry copy() {
        ARQLinkProviderRegistry result = new ARQLinkProviderRegistry();
        result.linkProviders.addAll(linkProviders);
        return result;
    }

    /** Create a copy of the registry from the context or return a new instance */
    public static ARQLinkProviderRegistry copyFrom(Context context) {
        ARQLinkProviderRegistry tmp = get(context);
        ARQLinkProviderRegistry result = tmp != null
                ? tmp.copy()
                : new ARQLinkProviderRegistry();
        return result;
    }

    public ARQLinkProviderRegistry() { }

    private static void init()
    {
        registry = new ARQLinkProviderRegistry();

        registry.add(new ARQLinkProviderMain());
    }

    // ----- Query -----

    /** Add a link provider to the default registry */
    public static void addProvider(ARQLinkProvider f) { get().add(f); }

    /** Add a query dispatcher */
    public void add(ARQLinkProvider f)
    {
        // Add to low end so that newer factories are tried first
        linkProviders.add(0, f);
    }

    /** Remove a query dispatcher */
    public static void removeProvider(ARQLinkProvider f)  { get().remove(f); }

    /** Remove a query dispatcher */
    public void remove(ARQLinkProvider f)  { linkProviders.remove(f); }

    /** Check whether a query dispatcher is already registered in the default registry */
    public static boolean containsFactory(ARQLinkProvider f) { return get().contains(f); }

    /** Check whether a query dispatcher is already registered */
    public boolean contains(ARQLinkProvider f) { return linkProviders.contains(f); }

    public static ARQLink connect(DatasetGraph dsg) {
        Objects.requireNonNull(dsg);

        Context cxt = dsg.getContext();
        ARQLinkProviderRegistry registry = chooseRegistry(cxt);

        ARQLink result = null;
        for (ARQLinkProvider provider : registry.linkProviders) {
            result = provider.connect(dsg);
            if (result != null) {
                break;
            }
        }
        Objects.requireNonNull(result, "No provider found for " + dsg.getClass());
        return result;
    }
}
