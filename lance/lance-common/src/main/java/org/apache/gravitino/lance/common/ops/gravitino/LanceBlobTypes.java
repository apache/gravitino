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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Converts Lance blob columns between their Arrow field form and the catalog strings used by
 * Gravitino external types.
 *
 * <ul>
 *   <li>{@code lance.blob}: a Lance blob v2 column, an Arrow {@code Struct<data: LargeBinary, uri:
 *       Utf8>} tagged with {@code ARROW:extension:name=lance.blob.v2}. It requires Lance file
 *       format version 2.2 or later.
 *   <li>{@code lance.blob.legacy}: a Lance legacy blob column, an Arrow {@code LargeBinary} field
 *       with metadata {@code lance-encoding:blob=true}. Lance rejects it from file format version
 *       2.2 on.
 * </ul>
 *
 * <p>Only the blob marker is recognized; other field metadata, such as storage thresholds, is not
 * represented. A blob field in any other layout is left to the Arrow JSON representation so that it
 * round-trips unchanged.
 */
final class LanceBlobTypes {

  static final String BLOB = "lance.blob";
  static final String LEGACY_BLOB = "lance.blob.legacy";

  static final String BLOB_META_KEY = "lance-encoding:blob";
  static final String ARROW_EXT_NAME_KEY = "ARROW:extension:name";
  static final String BLOB_V2_EXT_NAME = "lance.blob.v2";

  /** The Lance file format version from which blob v2 is supported and legacy blob is rejected. */
  static final String BLOB_FILE_FORMAT_VERSION = "2.2";

  /** The Lance file format version used for datasets with legacy blob columns. */
  static final String LEGACY_BLOB_FILE_FORMAT_VERSION = "2.1";

  private static final List<Field> BLOB_CHILDREN =
      ImmutableList.of(
          new Field("data", new FieldType(true, ArrowType.LargeBinary.INSTANCE, null), null),
          new Field("uri", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));

  private LanceBlobTypes() {}

  /**
   * Returns whether the field carries a Lance blob marker, either legacy blob or blob v2. This
   * matches Lance's own check, which only looks at whether the legacy key is present.
   *
   * @param field The Arrow field.
   * @return true if the field is a Lance blob field.
   */
  static boolean isBlob(Field field) {
    return isBlobV2(field) || isLegacyBlob(field);
  }

  /**
   * Returns the catalog string of a Lance blob field in its standard layout.
   *
   * @param field The Arrow field.
   * @return The catalog string, or empty if the field is not a Lance blob in its standard layout.
   */
  static Optional<String> toCatalogString(Field field) {
    if (field.getDictionary() != null) {
      return Optional.empty();
    }
    if (isBlobV2(field)
        && field.getType() instanceof ArrowType.Struct
        && childrenMatch(field.getChildren(), BLOB_CHILDREN)) {
      return Optional.of(BLOB);
    }
    if (isLegacyBlob(field)
        && field.getType() instanceof ArrowType.LargeBinary
        && field.getChildren().isEmpty()
        && "true".equals(field.getMetadata().get(BLOB_META_KEY))) {
      return Optional.of(LEGACY_BLOB);
    }
    return Optional.empty();
  }

  /**
   * Returns whether the catalog string names a Lance blob type.
   *
   * @param catalogString The external type catalog string.
   * @return true if the string starts with {@code lance.blob}.
   */
  static boolean isBlobCatalogString(String catalogString) {
    return catalogString != null && catalogString.trim().startsWith(BLOB);
  }

  /**
   * Builds the Arrow field described by a Lance blob catalog string.
   *
   * @param name The field name.
   * @param nullable Whether the field is nullable.
   * @param catalogString The Lance blob catalog string.
   * @return The Arrow field.
   * @throws IllegalArgumentException If the catalog string is not a Lance blob type.
   */
  static Field toArrowField(String name, boolean nullable, String catalogString) {
    switch (catalogString.trim()) {
      case BLOB:
        return new Field(
            name,
            new FieldType(
                nullable,
                ArrowType.Struct.INSTANCE,
                null,
                ImmutableMap.of(ARROW_EXT_NAME_KEY, BLOB_V2_EXT_NAME)),
            BLOB_CHILDREN);
      case LEGACY_BLOB:
        return new Field(
            name,
            new FieldType(
                nullable,
                ArrowType.LargeBinary.INSTANCE,
                null,
                ImmutableMap.of(BLOB_META_KEY, "true")),
            null);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported Lance blob type %s, expected %s or %s",
                catalogString.trim(), BLOB, LEGACY_BLOB));
    }
  }

  /**
   * Returns the Lance file format version that a new dataset with these fields must use.
   *
   * @param fields The top-level Arrow fields of the dataset.
   * @return {@value #BLOB_FILE_FORMAT_VERSION} if any field contains a blob v2 column, {@value
   *     #LEGACY_BLOB_FILE_FORMAT_VERSION} if any field contains a legacy blob column, or empty if
   *     there is no blob column.
   * @throws IllegalArgumentException If the fields contain both blob v2 and legacy blob columns.
   */
  static Optional<String> requiredFileFormatVersion(List<Field> fields) {
    boolean hasBlobV2 = fields.stream().anyMatch(field -> contains(field, true));
    boolean hasLegacyBlob = fields.stream().anyMatch(field -> contains(field, false));
    if (hasBlobV2 && hasLegacyBlob) {
      throw new IllegalArgumentException(
          String.format(
              "A Lance table cannot mix %s and %s columns: %s requires file format version %s or"
                  + " later, which does not support %s",
              BLOB, LEGACY_BLOB, BLOB, BLOB_FILE_FORMAT_VERSION, LEGACY_BLOB));
    }
    if (hasBlobV2) {
      return Optional.of(BLOB_FILE_FORMAT_VERSION);
    }
    return hasLegacyBlob ? Optional.of(LEGACY_BLOB_FILE_FORMAT_VERSION) : Optional.empty();
  }

  /**
   * Checks that a field can be added to a dataset with the given Lance file format version.
   *
   * @param field The Arrow field to add.
   * @param fileFormatVersion The dataset's Lance file format version, such as {@code 2.1}.
   * @throws IllegalArgumentException If the field contains a blob column the version does not
   *     support.
   */
  static void checkFileFormatVersion(Field field, String fileFormatVersion) {
    Optional<Boolean> supportsBlobV2 = supportsBlobV2(fileFormatVersion);
    if (supportsBlobV2.isEmpty()) {
      // Leave unknown versions to Lance.
      return;
    }
    if (!supportsBlobV2.get() && contains(field, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Column %s of type %s requires Lance file format version %s or later, but the"
                  + " dataset uses %s",
              field.getName(), BLOB, BLOB_FILE_FORMAT_VERSION, fileFormatVersion));
    }
    if (supportsBlobV2.get() && contains(field, false)) {
      throw new IllegalArgumentException(
          String.format(
              "Column %s of type %s is not supported by Lance file format version %s or later,"
                  + " but the dataset uses %s; use %s instead",
              field.getName(), LEGACY_BLOB, BLOB_FILE_FORMAT_VERSION, fileFormatVersion, BLOB));
    }
  }

  private static boolean isBlobV2(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null && BLOB_V2_EXT_NAME.equals(metadata.get(ARROW_EXT_NAME_KEY));
  }

  private static boolean isLegacyBlob(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null && metadata.containsKey(BLOB_META_KEY) && !isBlobV2(field);
  }

  private static boolean contains(Field field, boolean blobV2) {
    if (blobV2 ? isBlobV2(field) : isLegacyBlob(field)) {
      return true;
    }
    return field.getChildren().stream().anyMatch(child -> contains(child, blobV2));
  }

  private static Optional<Boolean> supportsBlobV2(String fileFormatVersion) {
    if (fileFormatVersion == null) {
      return Optional.empty();
    }
    String[] parts = fileFormatVersion.trim().split("\\.");
    if (parts.length != 2) {
      return Optional.empty();
    }
    try {
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);
      return Optional.of(major > 2 || (major == 2 && minor >= 2));
    } catch (NumberFormatException e) {
      return Optional.empty();
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
}
