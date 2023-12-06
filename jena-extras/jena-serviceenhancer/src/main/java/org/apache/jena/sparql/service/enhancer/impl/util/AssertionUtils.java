package org.apache.jena.sparql.service.enhancer.impl.util;

public class AssertionUtils {
    public static final boolean IS_ASSERT_ENABLED = isAssertEnabled();

    public static boolean isAssertEnabled() {
        boolean result;
        try {
           assert false;
           result = false;
        } catch (AssertionError e) {
           result = true;
        }
        return result;
    }
}
