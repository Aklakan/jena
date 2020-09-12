package org.apache.jena.dboe.storage.tuple;

import java.util.Map;

/**
 * Helper interface for creating new map instances with automatically inferred types
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public interface MapSupplier {
  <K, V> Map<K, V> newMap();
}
