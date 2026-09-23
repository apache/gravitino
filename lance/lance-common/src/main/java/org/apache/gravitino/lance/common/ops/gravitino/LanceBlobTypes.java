/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Converts Lance blob columns between their Arrow field form and a readable catalog string used by
 * Gravitino external types.
 *
 * <p>Supported catalog strings:
 *
 * <ul>
 *   <li>{@code lance.blob.v1}: an Arrow {@code LargeBinary} field with metadata {@code
 *       lance-encoding:blob=true}.
 *   <li>{@code lance.blob.v2(with_range=true, inline_size_threshold=N, dedicated_size_threshold=N,
 *       pack_file_size_threshold=N)}: an Arrow struct tagged with {@code
 *       ARROW:extension:name=lance.blob.v2}. All parameters are optional; without parameters the
 *       parentheses are omitted.
 * </ul>
 *
 * <p>Only Lance blob metadata is recognized; other field metadata is not represented. A blob field
 * that does not exactly match the canonical Lance layout is left to the Arrow JSON representation.
 */
final class LanceBlobTypes {

  static final String BLOB_META_KEY = "lance-encoding:blob";
  static final String ARROW_EXT_NAME_KEY = "ARROW:extension:name";
  static final String BLOB_V2_EXT_NAME = "lance.blob.v2";
  static final String INLINE_SIZE_THRESHOLD_META_KEY = "lance-encoding:blob-inline-size-threshold";
  static final String DEDICATED_SIZE_THRESHOLD_META_KEY =
      "lance-encoding:blob-dedicated-size-threshold";
  static final String PACK_FILE_SIZE_THRESHOLD_META_KEY =
      "lance-encoding:blob-pack-file-size-threshold";

  static final String V1 = "lance.blob.v1";
  static final String V2 = "lance.blob.v2";

  private static final String PREFIX = "lance.blob.";
  private static final String WITH_RANGE = "with_range";

  // Catalog string parameter name -> Arrow metadata key, in canonical output order.
  private static final Map<String, String> THRESHOLD_PARAMS =
      ImmutableMap.of(
          "inline_size_threshold", INLINE_SIZE_THRESHOLD_META_KEY,
          "dedicated_size_threshold", DEDICATED_SIZE_THRESHOLD_META_KEY,
          "pack_file_size_threshold", PACK_FILE_SIZE_THRESHOLD_META_KEY);

  // Catalog string parameter name -> minimum accepted value, matching Lance's validation.
  private static final Map<String, Long> THRESHOLD_MINIMUMS =
      ImmutableMap.of(
          "inline_size_threshold", 0L,
          "dedicated_size_threshold", 1L,
          "pack_file_size_threshold", 1L);

  private static final ArrowType UINT64 = new ArrowType.Int(64, false);

  private static final List<Field> V2_MINIMAL_CHILDREN =
      ImmutableList.of(
          nullableChild("data", ArrowType.LargeBinary.INSTANCE),
          nullableChild("uri", ArrowType.Utf8.INSTANCE));

  private static final List<Field> V2_FULL_CHILDREN =
      ImmutableList.<Field>builder()
          .addAll(V2_MINIMAL_CHILDREN)
          .add(nullableChild("position", UINT64))
          .add(nullableChild("size", UINT64))
          .build();

  private static final String SUPPORTED_FORMATS =
      V1
          + ", "
          + V2
          + "("
          + WITH_RANGE
          + "=true, "
          + String.join("=N, ", THRESHOLD_PARAMS.keySet())
          + "=N)";

  private LanceBlobTypes() {}

  /**
   * Returns whether the field carries Lance blob metadata, either legacy blob or blob v2.
   *
   * @param field The Arrow field.
   * @return true if the field is a Lance blob field.
   */
  static boolean isBlob(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null
        && (metadata.containsKey(BLOB_META_KEY)
            || BLOB_V2_EXT_NAME.equals(metadata.get(ARROW_EXT_NAME_KEY)));
  }

  /**
   * Returns the catalog string of a canonical Lance blob field.
   *
   * @param field The Arrow field.
   * @return The catalog string, or empty if the field is not a canonical Lance blob.
   */
  static Optional<String> toCatalogString(Field field) {
    Map<String, String> metadata = field.getMetadata();
    if (metadata == null || metadata.isEmpty()) {
      return Optional.empty();
    }
    if (isCanonicalV1(field, metadata)) {
      return Optional.of(V1);
    }
    return toV2CatalogString(field, metadata);
  }

  /**
   * Returns whether the catalog string uses the Lance blob format.
   *
   * @param catalogString The external type catalog string.
   * @return true if the string starts with {@code lance.blob.}.
   */
  static boolean isBlobCatalogString(String catalogString) {
    return catalogString != null && catalogString.trim().startsWith(PREFIX);
  }

  /**
   * Builds the Arrow field described by a Lance blob catalog string.
   *
   * @param name The field name.
   * @param nullable Whether the field is nullable.
   * @param catalogString The Lance blob catalog string.
   * @return The Arrow field.
   * @throws IllegalArgumentException If the catalog string is not a valid Lance blob type.
   */
  static Field toArrowField(String name, boolean nullable, String catalogString) {
    String trimmed = catalogString.trim();
    if (trimmed.equals(V1)) {
      return new Field(
          name,
          new FieldType(
              nullable,
              ArrowType.LargeBinary.INSTANCE,
              null,
              ImmutableMap.of(BLOB_META_KEY, "true")),
          null);
    }

    Preconditions.checkArgument(
        trimmed.startsWith(V2),
        "Unsupported Lance blob type %s, expected one of: %s",
        trimmed,
        SUPPORTED_FORMATS);
    String rest = trimmed.substring(V2.length()).trim();
    Map<String, String> params = parseParams(rest, trimmed);

    boolean withRange = false;
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(ARROW_EXT_NAME_KEY, BLOB_V2_EXT_NAME);
    for (Map.Entry<String, String> param : params.entrySet()) {
      String key = param.getKey();
      String value = param.getValue();
      if (key.equals(WITH_RANGE)) {
        Preconditions.checkArgument(
            value.equals("true") || value.equals("false"),
            "Invalid value %s for %s in Lance blob type %s, expected true or false",
            value,
            WITH_RANGE,
            trimmed);
        withRange = Boolean.parseBoolean(value);
      } else if (THRESHOLD_PARAMS.containsKey(key)) {
        metadata.put(THRESHOLD_PARAMS.get(key), parseThreshold(key, value, trimmed));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown parameter %s in Lance blob type %s, expected one of: %s",
                key, trimmed, SUPPORTED_FORMATS));
      }
    }

    return new Field(
        name,
        new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata),
        withRange ? V2_FULL_CHILDREN : V2_MINIMAL_CHILDREN);
  }

  private static boolean isCanonicalV1(Field field, Map<String, String> metadata) {
    return field.getType() instanceof ArrowType.LargeBinary
        && field.getDictionary() == null
        && field.getChildren().isEmpty()
        && "true".equals(metadata.get(BLOB_META_KEY));
  }

  private static Optional<String> toV2CatalogString(Field field, Map<String, String> metadata) {
    if (!(field.getType() instanceof ArrowType.Struct)
        || field.getDictionary() != null
        || !BLOB_V2_EXT_NAME.equals(metadata.get(ARROW_EXT_NAME_KEY))) {
      return Optional.empty();
    }

    List<Field> children = field.getChildren();
    boolean withRange;
    if (childrenMatch(children, V2_MINIMAL_CHILDREN)) {
      withRange = false;
    } else if (childrenMatch(children, V2_FULL_CHILDREN)) {
      withRange = true;
    } else {
      return Optional.empty();
    }

    List<String> params = new ArrayList<>();
    if (withRange) {
      params.add(WITH_RANGE + "=true");
    }
    for (Map.Entry<String, String> param : THRESHOLD_PARAMS.entrySet()) {
      String value = metadata.get(param.getValue());
      if (value == null) {
        continue;
      }
      if (!isCanonicalThreshold(param.getKey(), value)) {
        return Optional.empty();
      }
      params.add(param.getKey() + "=" + value);
    }

    return Optional.of(params.isEmpty() ? V2 : V2 + "(" + String.join(", ", params) + ")");
  }

  private static Map<String, String> parseParams(String rest, String catalogString) {
    Map<String, String> params = new LinkedHashMap<>();
    if (rest.isEmpty()) {
      return params;
    }
    Preconditions.checkArgument(
        rest.startsWith("(") && rest.endsWith(")"),
        "Unsupported Lance blob type %s, expected one of: %s",
        catalogString,
        SUPPORTED_FORMATS);
    String body = rest.substring(1, rest.length() - 1).trim();
    if (body.isEmpty()) {
      return params;
    }
    for (String part : body.split(",", -1)) {
      String[] kv = part.split("=", -1);
      Preconditions.checkArgument(
          kv.length == 2 && !kv[0].trim().isEmpty() && !kv[1].trim().isEmpty(),
          "Invalid parameter '%s' in Lance blob type %s, expected key=value",
          part.trim(),
          catalogString);
      String key = kv[0].trim();
      Preconditions.checkArgument(
          params.put(key, kv[1].trim()) == null,
          "Duplicate parameter %s in Lance blob type %s",
          key,
          catalogString);
    }
    return params;
  }

  private static String parseThreshold(String key, String value, String catalogString) {
    long threshold;
    try {
      threshold = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value %s for %s in Lance blob type %s, expected an integer",
              value, key, catalogString),
          e);
    }
    long min = THRESHOLD_MINIMUMS.get(key);
    Preconditions.checkArgument(
        threshold >= min,
        "Invalid value %s for %s in Lance blob type %s, expected an integer >= %s",
        value,
        key,
        catalogString,
        min);
    return Long.toString(threshold);
  }

  private static boolean isCanonicalThreshold(String key, String value) {
    try {
      long threshold = Long.parseLong(value);
      return Long.toString(threshold).equals(value) && threshold >= THRESHOLD_MINIMUMS.get(key);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  // Compares blob v2 children the way Lance classifies the layout: by name, Arrow type and
  // nullability. Child metadata is not part of the layout and is ignored.
  private static boolean childrenMatch(List<Field> actual, List<Field> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    for (int i = 0; i < actual.size(); i++) {
      Field actualChild = actual.get(i);
      Field expectedChild = expected.get(i);
      if (!actualChild.getName().equals(expectedChild.getName())
          || !actualChild.getType().equals(expectedChild.getType())
          || actualChild.isNullable() != expectedChild.isNullable()
          || actualChild.getDictionary() != null
          || !actualChild.getChildren().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static Field nullableChild(String name, ArrowType type) {
    return new Field(name, new FieldType(true, type, null), null);
  }
}
