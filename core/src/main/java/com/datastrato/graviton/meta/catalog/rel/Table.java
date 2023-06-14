package com.datastrato.graviton.meta.catalog.rel;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.meta.Auditable;
import com.datastrato.graviton.meta.catalog.TableCatalog;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a table in a {@link Namespace}. It defines basic properties of a table.
 * A catalog implementation with {@link TableCatalog} should implement it.
 */
public interface Table extends Entity, Auditable, HasIdentifier {

  /** return the name of the table. */
  @JsonProperty("name")
  String name();

  /** return the columns of the table. */
  @JsonProperty("columns")
  Column[] columns();

  /** return the comment of the table. Null is returned if no comment is set. */
  @Nullable
  @JsonProperty("comment")
  default String comment() {
    return null;
  }

  /** return the properties of the table. Empty map is returned if no properties are set. */
  @JsonProperty("properties")
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
