/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link FromIcebergPartitionSpec}. */
public class TestFromIcebergPartitionSpec extends TestBaseConvert {

  @Test
  public void testFormTransform() {
    Types.NestedField[] nestedFields = createNestedField("col_1", "col_2", "col_3");
    String col4Name = "col_4";
    Types.NestedField col4Field =
        Types.NestedField.optional(4, col4Name, Types.DateType.get(), TEST_COMMENT);
    nestedFields = ArrayUtils.add(nestedFields, col4Field);

    Schema schema = new Schema(nestedFields);
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    PartitionSpec partitionSpec = builder.day(col4Name).identity("col_1").build();
    Transform[] transforms = FromIcebergPartitionSpec.fromPartitionSpec(partitionSpec, schema);
    Assertions.assertEquals(2, transforms.length);
    Assertions.assertEquals("day", transforms[0].name());
    Assertions.assertTrue(transforms[0] instanceof Transforms.FunctionTrans);
    Assertions.assertEquals(
        "col_4", ((Transforms.NamedReference) transforms[0].arguments()[0]).value()[0]);
    Assertions.assertTrue(transforms[1] instanceof Transforms.NamedReference);
  }
}
