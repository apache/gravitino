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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestLanceBlobTypes {

  private static final Map<String, String> LEGACY_META =
      Map.of(LanceBlobTypes.BLOB_META_KEY, "true");

  @Test
  void testBlobRoundTrip() {
    Field field = blobField(standardChildren(), Map.of());

    assertEquals(Optional.of("lance.blob"), LanceBlobTypes.toCatalogString(field));
    assertEquals(field, LanceBlobTypes.toArrowField("blob", true, "lance.blob"));
  }

  @Test
  void testLegacyBlobRoundTrip() {
    Field field =
        new Field(
            "image", new FieldType(false, ArrowType.LargeBinary.INSTANCE, null, LEGACY_META), null);

    assertEquals(Optional.of("lance.blob.legacy"), LanceBlobTypes.toCatalogString(field));
    assertEquals(field, LanceBlobTypes.toArrowField("image", false, "lance.blob.legacy"));
  }

  @Test
  void testNonLayoutMetadataIsIgnored() {
    Field legacy =
        new Field(
            "b",
            new FieldType(
                true,
                ArrowType.LargeBinary.INSTANCE,
                null,
                Map.of(LanceBlobTypes.BLOB_META_KEY, "true", "other", "x")),
            null);
    assertEquals(Optional.of("lance.blob.legacy"), LanceBlobTypes.toCatalogString(legacy));

    // Storage thresholds and pyarrow's ARROW:extension:metadata entry.
    Field blob =
        blobField(
            standardChildren(),
            Map.of(
                "ARROW:extension:metadata", "",
                "lance-encoding:blob-dedicated-size-threshold", "16"));
    assertEquals(Optional.of("lance.blob"), LanceBlobTypes.toCatalogString(blob));

    // Child metadata is not part of the blob v2 layout.
    List<Field> childWithMetadata =
        Arrays.asList(
            new Field(
                "data",
                new FieldType(
                    true,
                    ArrowType.LargeBinary.INSTANCE,
                    null,
                    Map.of("lance-encoding:compression", "zstd")),
                null),
            standardChildren().get(1));
    assertEquals(
        Optional.of("lance.blob"),
        LanceBlobTypes.toCatalogString(blobField(childWithMetadata, Map.of())));
  }

  @Test
  void testNonStandardLegacyBlobIsNotReadable() {
    assertFalse(
        LanceBlobTypes.toCatalogString(
                new Field(
                    "b", new FieldType(true, ArrowType.Binary.INSTANCE, null, LEGACY_META), null))
            .isPresent());
    assertFalse(
        LanceBlobTypes.toCatalogString(
                new Field(
                    "b",
                    new FieldType(
                        true,
                        ArrowType.LargeBinary.INSTANCE,
                        null,
                        Map.of(LanceBlobTypes.BLOB_META_KEY, "false")),
                    null))
            .isPresent());
    assertFalse(
        LanceBlobTypes.toCatalogString(
                new Field(
                    "b",
                    new FieldType(
                        true,
                        ArrowType.LargeBinary.INSTANCE,
                        new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)),
                        LEGACY_META),
                    null))
            .isPresent());
  }

  @Test
  void testNonStandardBlobIsNotReadable() {
    // With the optional external range fields.
    List<Field> fullChildren = new ArrayList<>(standardChildren());
    fullChildren.add(
        new Field("position", new FieldType(true, new ArrowType.Int(64, false), null), null));
    fullChildren.add(
        new Field("size", new FieldType(true, new ArrowType.Int(64, false), null), null));
    assertFalse(LanceBlobTypes.toCatalogString(blobField(fullChildren, Map.of())).isPresent());

    // Wrong child order.
    List<Field> reordered = new ArrayList<>(standardChildren());
    Collections.reverse(reordered);
    assertFalse(LanceBlobTypes.toCatalogString(blobField(reordered, Map.of())).isPresent());

    // Non-nullable child.
    List<Field> nonNullable =
        Arrays.asList(
            new Field("data", new FieldType(false, ArrowType.LargeBinary.INSTANCE, null), null),
            standardChildren().get(1));
    assertFalse(LanceBlobTypes.toCatalogString(blobField(nonNullable, Map.of())).isPresent());

    // Dictionary-encoded child.
    List<Field> dictionaryChild =
        Arrays.asList(
            standardChildren().get(0),
            new Field(
                "uri",
                new FieldType(
                    true,
                    ArrowType.Utf8.INSTANCE,
                    new DictionaryEncoding(1L, false, new ArrowType.Int(32, true))),
                null));
    assertFalse(LanceBlobTypes.toCatalogString(blobField(dictionaryChild, Map.of())).isPresent());

    // Not a struct.
    assertFalse(
        LanceBlobTypes.toCatalogString(
                new Field(
                    "blob",
                    new FieldType(
                        true,
                        ArrowType.LargeBinary.INSTANCE,
                        null,
                        Map.of(LanceBlobTypes.ARROW_EXT_NAME_KEY, LanceBlobTypes.BLOB_V2_EXT_NAME)),
                    null))
            .isPresent());
  }

  @Test
  void testFieldWithoutBlobMarkerIsNotBlob() {
    Field struct = new Field("s", new FieldType(true, ArrowType.Struct.INSTANCE, null), null);
    assertFalse(LanceBlobTypes.isBlob(struct));
    assertFalse(LanceBlobTypes.toCatalogString(struct).isPresent());
    assertFalse(
        LanceBlobTypes.isBlob(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.LargeBinary.INSTANCE,
                    null,
                    Map.of("lance-encoding:blob-dedicated-size-threshold", "1")),
                null)));
  }

  @Test
  void testIsBlob() {
    assertTrue(LanceBlobTypes.isBlob(LanceBlobTypes.toArrowField("b", true, "lance.blob")));
    assertTrue(LanceBlobTypes.isBlob(LanceBlobTypes.toArrowField("b", true, "lance.blob.legacy")));
    // Lance only checks that the legacy key is present.
    assertTrue(
        LanceBlobTypes.isBlob(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.Binary.INSTANCE,
                    null,
                    Map.of(LanceBlobTypes.BLOB_META_KEY, "false")),
                null)));
  }

  @Test
  void testIsBlobCatalogString() {
    assertTrue(LanceBlobTypes.isBlobCatalogString("lance.blob"));
    assertTrue(LanceBlobTypes.isBlobCatalogString(" lance.blob.legacy "));
    assertTrue(LanceBlobTypes.isBlobCatalogString("lance.blob.v2"));
    assertFalse(LanceBlobTypes.isBlobCatalogString("{\"name\":\"lance.blob\"}"));
    assertFalse(LanceBlobTypes.isBlobCatalogString(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"lance.blob.v1", "lance.blob.v2", "lance.blob(x=1)", "lance.blobs"})
  void testInvalidCatalogStrings(String catalogString) {
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceBlobTypes.toArrowField("blob", true, catalogString));
  }

  @Test
  void testRequiredFileFormatVersion() {
    Field id = new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null);
    Field blob = LanceBlobTypes.toArrowField("blob", true, "lance.blob");
    Field legacy = LanceBlobTypes.toArrowField("legacy", true, "lance.blob.legacy");
    Field nestedBlob =
        new Field(
            "record",
            new FieldType(true, ArrowType.Struct.INSTANCE, null),
            Collections.singletonList(blob));

    assertEquals(Optional.empty(), LanceBlobTypes.requiredFileFormatVersion(List.of(id)));
    assertEquals(Optional.of("2.2"), LanceBlobTypes.requiredFileFormatVersion(List.of(id, blob)));
    assertEquals(
        Optional.of("2.2"), LanceBlobTypes.requiredFileFormatVersion(List.of(id, nestedBlob)));
    assertEquals(Optional.of("2.1"), LanceBlobTypes.requiredFileFormatVersion(List.of(id, legacy)));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceBlobTypes.requiredFileFormatVersion(List.of(blob, legacy)));
  }

  @Test
  void testCheckFileFormatVersion() {
    Field id = new Field("id", new FieldType(true, new ArrowType.Int(32, true), null), null);
    Field blob = LanceBlobTypes.toArrowField("blob", true, "lance.blob");
    Field legacy = LanceBlobTypes.toArrowField("legacy", true, "lance.blob.legacy");

    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(id, "2.1"));
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(blob, "2.2"));
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(blob, "2.3"));
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(legacy, "2.1"));
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(legacy, "0.1"));
    // Unknown versions are left to Lance.
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(blob, "stable"));
    assertDoesNotThrow(() -> LanceBlobTypes.checkFileFormatVersion(blob, null));

    IllegalArgumentException blobError =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceBlobTypes.checkFileFormatVersion(blob, "2.1"));
    assertTrue(blobError.getMessage().contains("2.2"), blobError.getMessage());
    assertThrows(
        IllegalArgumentException.class, () -> LanceBlobTypes.checkFileFormatVersion(legacy, "2.2"));
  }

  private static Field blobField(List<Field> children, Map<String, String> extraMetadata) {
    Map<String, String> metadata = new HashMap<>(extraMetadata);
    metadata.put(LanceBlobTypes.ARROW_EXT_NAME_KEY, LanceBlobTypes.BLOB_V2_EXT_NAME);
    return new Field(
        "blob", new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata), children);
  }

  private static List<Field> standardChildren() {
    return Arrays.asList(
        new Field("data", new FieldType(true, ArrowType.LargeBinary.INSTANCE, null), null),
        new Field("uri", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));
  }
}
