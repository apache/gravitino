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

  @Test
  void testV1RoundTrip() {
    Field field =
        new Field(
            "image",
            new FieldType(
                false,
                ArrowType.LargeBinary.INSTANCE,
                null,
                Map.of(LanceBlobTypes.BLOB_META_KEY, "true")),
            null);

    assertEquals(Optional.of("lance.blob.v1"), LanceBlobTypes.toCatalogString(field));
    assertEquals(field, LanceBlobTypes.toArrowField("image", false, "lance.blob.v1"));
  }

  @Test
  void testNonCanonicalV1FallsBack() {
    Map<String, String> blobMeta = Map.of(LanceBlobTypes.BLOB_META_KEY, "true");
    assertEquals(
        Optional.empty(),
        LanceBlobTypes.toCatalogString(
            new Field("b", new FieldType(true, ArrowType.Binary.INSTANCE, null, blobMeta), null)));
    assertEquals(
        Optional.empty(),
        LanceBlobTypes.toCatalogString(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.LargeBinary.INSTANCE,
                    new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)),
                    blobMeta),
                null)));
    assertEquals(
        Optional.empty(),
        LanceBlobTypes.toCatalogString(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.LargeBinary.INSTANCE,
                    null,
                    Map.of(LanceBlobTypes.BLOB_META_KEY, "yes")),
                null)));
  }

  @Test
  void testNonBlobMetadataIsIgnored() {
    Field v1 =
        new Field(
            "b",
            new FieldType(
                true,
                ArrowType.LargeBinary.INSTANCE,
                null,
                Map.of(LanceBlobTypes.BLOB_META_KEY, "true", "other", "x")),
            null);
    assertEquals(Optional.of("lance.blob.v1"), LanceBlobTypes.toCatalogString(v1));

    // pyarrow extension types always export an ARROW:extension:metadata entry.
    Field v2 =
        v2Field(
            minimalChildren(),
            Map.of(
                "ARROW:extension:metadata",
                "",
                "lance-schema:unenforced-primary-key",
                "true",
                LanceBlobTypes.INLINE_SIZE_THRESHOLD_META_KEY,
                "16"));
    assertEquals(
        Optional.of("lance.blob.v2(inline_size_threshold=16)"), LanceBlobTypes.toCatalogString(v2));
  }

  @Test
  void testIsBlob() {
    assertTrue(LanceBlobTypes.isBlob(LanceBlobTypes.toArrowField("b", true, "lance.blob.v1")));
    assertTrue(LanceBlobTypes.isBlob(LanceBlobTypes.toArrowField("b", true, "lance.blob.v2")));
    assertTrue(
        LanceBlobTypes.isBlob(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.Binary.INSTANCE,
                    null,
                    Map.of(LanceBlobTypes.BLOB_META_KEY, "true")),
                null)));
    assertFalse(
        LanceBlobTypes.isBlob(
            new Field(
                "b",
                new FieldType(
                    true,
                    ArrowType.LargeBinary.INSTANCE,
                    null,
                    Map.of(LanceBlobTypes.INLINE_SIZE_THRESHOLD_META_KEY, "1")),
                null)));
    assertFalse(
        LanceBlobTypes.isBlob(
            new Field("b", new FieldType(true, ArrowType.LargeBinary.INSTANCE, null), null)));
  }

  @Test
  void testV2MinimalRoundTrip() {
    Field field = v2Field(minimalChildren(), Map.of());

    assertEquals(Optional.of("lance.blob.v2"), LanceBlobTypes.toCatalogString(field));
    assertEquals(field, LanceBlobTypes.toArrowField("blob", true, "lance.blob.v2"));
  }

  @Test
  void testV2WithAllParamsRoundTrip() {
    Field field =
        v2Field(
            fullChildren(),
            Map.of(
                LanceBlobTypes.INLINE_SIZE_THRESHOLD_META_KEY, "0",
                LanceBlobTypes.DEDICATED_SIZE_THRESHOLD_META_KEY, "1048576",
                LanceBlobTypes.PACK_FILE_SIZE_THRESHOLD_META_KEY, "67108864"));
    String expected =
        "lance.blob.v2(with_range=true, inline_size_threshold=0, "
            + "dedicated_size_threshold=1048576, pack_file_size_threshold=67108864)";

    assertEquals(Optional.of(expected), LanceBlobTypes.toCatalogString(field));
    assertEquals(field, LanceBlobTypes.toArrowField("blob", true, expected));
  }

  @Test
  void testV2ParsesWithReorderedParamsAndWhitespace() {
    Field expected =
        v2Field(minimalChildren(), Map.of(LanceBlobTypes.DEDICATED_SIZE_THRESHOLD_META_KEY, "10"));

    assertEquals(
        expected,
        LanceBlobTypes.toArrowField(
            "blob", true, "  lance.blob.v2 ( dedicated_size_threshold = 10 , with_range=false ) "));
    assertEquals(
        v2Field(minimalChildren(), Map.of()),
        LanceBlobTypes.toArrowField("blob", true, "lance.blob.v2()"));
  }

  @Test
  void testNonCanonicalV2FallsBack() {
    // Wrong child order.
    List<Field> reordered = new ArrayList<>(minimalChildren());
    Collections.reverse(reordered);
    assertFalse(LanceBlobTypes.toCatalogString(v2Field(reordered, Map.of())).isPresent());

    // Non-nullable child.
    List<Field> nonNullable =
        Arrays.asList(
            new Field("data", new FieldType(false, ArrowType.LargeBinary.INSTANCE, null), null),
            minimalChildren().get(1));
    assertFalse(LanceBlobTypes.toCatalogString(v2Field(nonNullable, Map.of())).isPresent());

    // Child with metadata.
    List<Field> childWithMetadata =
        Arrays.asList(
            new Field(
                "data",
                new FieldType(true, ArrowType.LargeBinary.INSTANCE, null, Map.of("k", "v")),
                null),
            minimalChildren().get(1));
    assertFalse(LanceBlobTypes.toCatalogString(v2Field(childWithMetadata, Map.of())).isPresent());

    // Dictionary-encoded struct.
    Field dictionaryEncoded =
        new Field(
            "blob",
            new FieldType(
                true,
                ArrowType.Struct.INSTANCE,
                new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)),
                Map.of(LanceBlobTypes.ARROW_EXT_NAME_KEY, LanceBlobTypes.BLOB_V2_EXT_NAME)),
            minimalChildren());
    assertFalse(LanceBlobTypes.toCatalogString(dictionaryEncoded).isPresent());

    // Thresholds out of the range accepted by the readable form.
    for (Map.Entry<String, String> threshold :
        Map.of(
                LanceBlobTypes.INLINE_SIZE_THRESHOLD_META_KEY, "-1",
                LanceBlobTypes.DEDICATED_SIZE_THRESHOLD_META_KEY, "0",
                LanceBlobTypes.PACK_FILE_SIZE_THRESHOLD_META_KEY, "0")
            .entrySet()) {
      assertFalse(
          LanceBlobTypes.toCatalogString(
                  v2Field(minimalChildren(), Map.of(threshold.getKey(), threshold.getValue())))
              .isPresent(),
          threshold.toString());
    }

    // Non-canonical threshold value.
    assertFalse(
        LanceBlobTypes.toCatalogString(
                v2Field(
                    minimalChildren(),
                    Map.of(LanceBlobTypes.INLINE_SIZE_THRESHOLD_META_KEY, "04096")))
            .isPresent());
  }

  @Test
  void testFieldWithoutBlobMetadataIsNotBlob() {
    assertFalse(
        LanceBlobTypes.toCatalogString(
                new Field("s", new FieldType(true, ArrowType.Struct.INSTANCE, null), null))
            .isPresent());
  }

  @Test
  void testIsBlobCatalogString() {
    assertTrue(LanceBlobTypes.isBlobCatalogString("lance.blob.v1"));
    assertTrue(LanceBlobTypes.isBlobCatalogString("lance.blob.v2(with_range=true)"));
    assertTrue(LanceBlobTypes.isBlobCatalogString(" lance.blob.v1 "));
    assertFalse(LanceBlobTypes.isBlobCatalogString("{\"name\":\"lance.blob.v1\"}"));
    assertFalse(LanceBlobTypes.isBlobCatalogString(null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "lance.blob.v3",
        "lance.blob.v1(with_range=true)",
        "lance.blob.v2(unknown=1)",
        "lance.blob.v2(with_range=yes)",
        "lance.blob.v2(inline_size_threshold=abc)",
        "lance.blob.v2(inline_size_threshold=-1)",
        "lance.blob.v2(dedicated_size_threshold=0)",
        "lance.blob.v2(pack_file_size_threshold=0)",
        "lance.blob.v2(inline_size_threshold=1, inline_size_threshold=2)",
        "lance.blob.v2(inline_size_threshold)",
        "lance.blob.v2(inline_size_threshold=1",
        "lance.blob.v2x"
      })
  void testInvalidCatalogStrings(String catalogString) {
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceBlobTypes.toArrowField("blob", true, catalogString));
  }

  private static Field v2Field(List<Field> children, Map<String, String> extraMetadata) {
    Map<String, String> metadata = new HashMap<>(extraMetadata);
    metadata.put(LanceBlobTypes.ARROW_EXT_NAME_KEY, LanceBlobTypes.BLOB_V2_EXT_NAME);
    return new Field(
        "blob", new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata), children);
  }

  private static List<Field> minimalChildren() {
    return Arrays.asList(
        new Field("data", new FieldType(true, ArrowType.LargeBinary.INSTANCE, null), null),
        new Field("uri", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));
  }

  private static List<Field> fullChildren() {
    List<Field> children = new ArrayList<>(minimalChildren());
    children.add(
        new Field("position", new FieldType(true, new ArrowType.Int(64, false), null), null));
    children.add(new Field("size", new FieldType(true, new ArrowType.Int(64, false), null), null));
    return children;
  }
}
