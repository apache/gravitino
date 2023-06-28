package com.datastrato.graviton;

import java.util.Map;

/**
 * The interface of a lakehouse. The lakehouse is the top level entity in the graviton system. It
 * contains a set of catalogs.
 */
public interface Lakehouse extends Auditable {

  /** The name of the lakehouse. */
  String name();

  /** The comment of the lakehouse. */
  String comment();

  /** The properties of the lakehouse. */
  Map<String, String> properties();
}
