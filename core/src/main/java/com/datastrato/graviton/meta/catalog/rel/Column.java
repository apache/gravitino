package com.datastrato.graviton.meta.catalog.rel;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.json.JsonUtils;
import com.datastrato.graviton.meta.Auditable;
import com.datastrato.graviton.meta.catalog.TableCatalog;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.substrait.type.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type.
 *
 * <p>Catalog implementation needs to implement it. They should consume it in APIs like {@link
 * TableCatalog#createTable(NameIdentifier, Column[], String, Map)}, and report it in {@link
 * Table#columns()} a default value and a generation expression.
 */
public interface Column extends Entity, Auditable, HasIdentifier {

  /** Returns the name of this column. */
  @JsonProperty("name")
  String name();

  /** Returns the data type of this column. */
  @JsonProperty("type")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  Type dataType();

  /** Returns the comment of this column, null if not specified. */
  @Nullable
  @JsonProperty("comment")
  String comment();

  // TODO. Support column default value. @Jerry
}
