package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Set;

import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.service.enhancer.init.ServiceEnhancerConstants;

/** Domain adaption for the ServiceEnhancer executor. */
public class ServiceOptsSE {

    public static final String SO_BULK = "bulk";
    public static final String SO_CACHE = "cache";

    // Reserved;currently not implemented
    public static final String SO_LATERAL = "lateral";
    public static final String SO_LOOP = "loop";

    public static final String SO_CONCURRENT = "concurrent";
    public static final String SO_OPTIMIZE = "optimize";

    private static Set<String> knownOptions = Set.of(
        SO_BULK,
        SO_CACHE,
        SO_LATERAL,
        SO_LOOP,
        SO_CONCURRENT,
        SO_OPTIMIZE);

    public Set<String> getKnownOptions() {
        return knownOptions;
    }

    private static boolean isKnownOption(String key) {
        return knownOptions.contains(key);
    }

    public static ServiceOpts getEffectiveService(OpService opService) {
        return ServiceOpts.getEffectiveService(opService, ServiceEnhancerConstants.SELF.getURI(), ServiceOptsSE::isKnownOption);
    }
}
