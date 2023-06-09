package com.datastrato.graviton.meta.catalog.meta;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.meta.catalog.TableCatalog;
import com.datastrato.graviton.json.JsonUtils;
import com.datastrato.graviton.meta.Auditable;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.substrait.type.Type;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type.
 *
 *  Catalog implementation needs to implement it. They should consume it in APIs like
 * {@link TableCatalog#createTable(NameIdentifier, Column[], String, Map)}, and report it in
 * {@link Table#columns()}h a default value and a generation expression.
 */
public interface Column extends Entity, Auditable, HasIdentifier {

  /**
   * Returns the name of this column.
   */
  @JsonProperty("name")
  String name();

  /**
   * Returns the data type of this column.
   */
  @JsonProperty("type")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  Type dataType();

  /**
   * Returns whether this column allows null values.
   */
  @JsonProperty("nullable")
  default boolean nullable() {
    return dataType().nullable();
  }

  /**
   * Returns the comment of this column, null if not specified.
   */
  @Nullable
  @JsonProperty("comment")
  String comment();

  // TODO. Support column default value. @Jerry
}
