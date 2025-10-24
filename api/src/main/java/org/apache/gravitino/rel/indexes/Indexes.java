/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.rel.indexes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

/** Helper methods to create index to pass into Apache Gravitino. */
public class Indexes {

  /** An empty array of indexes. */
  public static final Index[] EMPTY_INDEXES = new Index[0];

  /** MySQL does not support setting the name of the primary key, so the default name is used. */
  public static final String DEFAULT_MYSQL_PRIMARY_KEY_NAME = "PRIMARY";

  /**
   * Create a unique index on columns. Like unique (a) or unique (a, b), for complex like unique
   *
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The unique index
   */
  public static Index unique(String name, String[][] fieldNames) {
    return of(Index.IndexType.UNIQUE_KEY, name, fieldNames);
  }

  /**
   * To create a MySQL primary key, you need to use the default primary key name.
   *
   * @param fieldNames The field names under the table contained in the index.
   * @return The primary key index
   */
  public static Index createMysqlPrimaryKey(String[][] fieldNames) {
    return primary(DEFAULT_MYSQL_PRIMARY_KEY_NAME, fieldNames);
  }

  /**
   * Create a primary index on columns. Like primary (a), for complex like primary
   *
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The primary index
   */
  public static Index primary(String name, String[][] fieldNames) {
    return of(Index.IndexType.PRIMARY_KEY, name, fieldNames);
  }

  /**
   * @param indexType The type of the index
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The index
   */
  public static Index of(Index.IndexType indexType, String name, String[][] fieldNames) {
    return IndexImpl.builder()
        .withIndexType(indexType)
        .withName(name)
        .withFieldNames(fieldNames)
        .build();
  }

  /** Custom JSON serializer for Index objects. */
  public static class IndexSerializer extends JsonSerializer<Index> {
    @Override
    public void serialize(Index value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("indexType", value.type().name().toUpperCase(Locale.ROOT));
      if (null != value.name()) {
        gen.writeStringField("name", value.name());
      }
      gen.writeFieldName("fieldNames");
      gen.writeObject(value.fieldNames());
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for Index objects. */
  public static class IndexDeserializer extends JsonDeserializer<Index> {

    @Override
    public Index deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Index must be a valid JSON object, but found: %s",
          node);

      IndexImpl.Builder builder = IndexImpl.builder();
      Preconditions.checkArgument(
          node.has("indexType"), "Cannot parse index from missing type: %s", node);
      String indexType = getString("indexType", node);
      builder.withIndexType(Index.IndexType.valueOf(indexType.toUpperCase(Locale.ROOT)));
      if (node.has("name")) {
        builder.withName(getString("name", node));
      }
      Preconditions.checkArgument(
          node.has("fieldNames"), "Cannot parse index from missing field names: %s", node);
      List<String[]> fieldNames = Lists.newArrayList();
      node.get("fieldNames").forEach(field -> fieldNames.add(getStringArray((ArrayNode) field)));
      builder.withFieldNames(fieldNames.toArray(new String[0][0]));
      return builder.build();
    }

    private static String[] getStringArray(ArrayNode node) {
      String[] array = new String[node.size()];
      for (int i = 0; i < node.size(); i++) {
        array[i] = node.get(i).asText();
      }
      return array;
    }

    private static String getString(String property, JsonNode node) {
      Preconditions.checkArgument(node.has(property), "Cannot parse missing string: %s", property);
      JsonNode pNode = node.get(property);
      return convertToString(property, pNode);
    }

    private static String convertToString(String property, JsonNode pNode) {
      Preconditions.checkArgument(
          pNode != null && !pNode.isNull() && pNode.isTextual(),
          "Cannot parse to a string value %s: %s",
          property,
          pNode);
      return pNode.asText();
    }
  }

  /** The user side implementation of the index. */
  @JsonSerialize(using = IndexSerializer.class)
  @JsonDeserialize(using = IndexDeserializer.class)
  public static final class IndexImpl implements Index {
    private final IndexType indexType;

    private final String name;

    private final String[][] fieldNames;

    /**
     * The constructor of the index.
     *
     * @param indexType The type of the index
     * @param name The name of the index
     * @param fieldNames The field names under the table contained in the index.
     */
    private IndexImpl(IndexType indexType, String name, String[][] fieldNames) {
      this.indexType = indexType;
      this.name = name;
      this.fieldNames = fieldNames;
    }

    /**
     * @return The type of the index
     */
    @Override
    public IndexType type() {
      return indexType;
    }

    /**
     * @return The name of the index
     */
    @Override
    public String name() {
      return name;
    }

    /**
     * @return The field names under the table contained in the index
     */
    @Override
    public String[][] fieldNames() {
      return fieldNames;
    }

    /**
     * @return the builder for creating a new instance of IndexImpl.
     */
    public static Builder builder() {
      return new Builder();
    }

    /** Builder to create an index. */
    public static class Builder {

      /** The type of the index. */
      protected IndexType indexType;

      /** The name of the index. */
      protected String name;

      /** The field names of the index. */
      protected String[][] fieldNames;

      /**
       * Set the type of the index.
       *
       * @param indexType The type of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withIndexType(IndexType indexType) {
        this.indexType = indexType;
        return this;
      }

      /**
       * Set the name of the index.
       *
       * @param name The name of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withName(String name) {
        this.name = name;
        return this;
      }

      /**
       * Set the field names of the index.
       *
       * @param fieldNames The field names of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withFieldNames(String[][] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
      }

      /**
       * Build a new instance of IndexImpl.
       *
       * @return The new instance.
       */
      public Index build() {
        return new IndexImpl(indexType, name, fieldNames);
      }
    }
  }

  private Indexes() {}
}
