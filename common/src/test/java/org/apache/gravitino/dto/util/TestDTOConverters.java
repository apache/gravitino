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

package org.apache.gravitino.dto.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.partitioning.ListPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import org.apache.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.ListPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDTOConverters {

  @Test
  void testIdentityPartitionDTOConvert() {

    // given
    String[] field1 = {"dt"};
    String[] field2 = {"country"};
    LiteralDTO literal1 =
        LiteralDTO.builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO literal2 =
        LiteralDTO.builder().withDataType(Types.StringType.get()).withValue("us").build();
    String[][] fieldNames = {field1, field2};
    LiteralDTO[] values = {literal1, literal2};
    Literal<?>[] expectedValues = {
      (Literal<?>) DTOConverters.fromFunctionArg(literal1),
      (Literal<?>) DTOConverters.fromFunctionArg(literal2)
    };

    Map<String, String> properties = Collections.singletonMap("key", "value");
    PartitionDTO identityPartitionDTO =
        IdentityPartitionDTO.builder()
            .withFieldNames(fieldNames)
            .withName("IdentityPartition")
            .withValues(values)
            .withProperties(properties)
            .build();
    // when
    IdentityPartition identityPartition =
        (IdentityPartition) DTOConverters.fromDTO(identityPartitionDTO);

    // then
    Assertions.assertTrue(Arrays.equals(fieldNames, identityPartition.fieldNames()));
    Assertions.assertEquals("IdentityPartition", identityPartition.name());
    Assertions.assertTrue(Arrays.equals(expectedValues, identityPartition.values()));
    Assertions.assertEquals(properties, identityPartition.properties());
  }

  @Test
  void testRangePartitionDTOConvert() {

    // given
    LiteralDTO lower =
        LiteralDTO.builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO upper =
        LiteralDTO.builder().withDataType(Types.StringType.get()).withValue("us").build();
    Literal<?> expectedLower = (Literal<?>) DTOConverters.fromFunctionArg(lower);
    Literal<?> expectedUpper = (Literal<?>) DTOConverters.fromFunctionArg(upper);

    Map<String, String> properties = Collections.singletonMap("key", "value");
    PartitionDTO rangePartitionDTO =
        RangePartitionDTO.builder()
            .withName("RangePartition")
            .withLower(lower)
            .withProperties(properties)
            .withUpper(upper)
            .build();
    // when
    RangePartition rangePartition = (RangePartition) DTOConverters.fromDTO(rangePartitionDTO);

    // then
    Assertions.assertEquals("RangePartition", rangePartition.name());
    Assertions.assertEquals(expectedLower, rangePartition.lower());
    Assertions.assertEquals(expectedUpper, rangePartition.upper());
    Assertions.assertEquals(properties, rangePartition.properties());
  }

  @Test
  void testListPartitionDTOConvert() {

    // given
    LiteralDTO literal1 =
        LiteralDTO.builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO literal2 =
        LiteralDTO.builder().withDataType(Types.StringType.get()).withValue("us").build();

    Map<String, String> properties = Collections.singletonMap("key", "value");
    LiteralDTO[][] literalDTOs = {new LiteralDTO[] {literal1}, new LiteralDTO[] {literal2}};
    Literal<?>[][] expectedValues = {
      new Literal<?>[] {(Literal<?>) DTOConverters.fromFunctionArg(literal1)},
      new Literal<?>[] {(Literal<?>) DTOConverters.fromFunctionArg(literal2)}
    };
    ListPartitionDTO listPartitionDTO =
        ListPartitionDTO.builder()
            .withName("ListPartition")
            .withLists(literalDTOs)
            .withProperties(properties)
            .build();

    // when
    ListPartition listPartition = (ListPartition) DTOConverters.fromDTO(listPartitionDTO);

    // then
    Assertions.assertEquals("ListPartition", listPartition.name());
    Assertions.assertTrue(Arrays.deepEquals(expectedValues, listPartition.lists()));
    Assertions.assertEquals(properties, listPartition.properties());
  }

  @Test
  void testIdentityPartitionConvert() {

    // given
    String[] field1 = {"dt"};
    String[] field2 = {"country"};
    Literal<?> literal1 = Literals.stringLiteral("2008-08-08");
    Literal<?> literal2 = Literals.stringLiteral("us");

    String[][] fieldNames = {field1, field2};
    Literal<?>[] values = {literal1, literal2};

    Map<String, String> properties = Collections.singletonMap("key", "value");
    Partition identityPartition = Partitions.identity("identity", fieldNames, values, properties);

    // when
    IdentityPartitionDTO partitionDTO =
        (IdentityPartitionDTO) DTOConverters.toDTO(identityPartition);

    // then
    Assertions.assertEquals("identity", partitionDTO.name());
    Assertions.assertEquals(PartitionDTO.Type.IDENTITY, partitionDTO.type());
    Assertions.assertTrue(Arrays.equals(fieldNames, partitionDTO.fieldNames()));
    Assertions.assertEquals(properties, partitionDTO.properties());
  }

  @Test
  void testRangePartitionConvert() {

    // given
    Literal<?> lower = Literals.stringLiteral("2008-08-08");
    Literal<?> upper = Literals.stringLiteral("us");

    Map<String, String> properties = Collections.singletonMap("key", "value");
    Partition identityPartition = Partitions.range("range", upper, lower, properties);

    // when
    RangePartitionDTO rangePartitionDTO =
        (RangePartitionDTO) DTOConverters.toDTO(identityPartition);

    // then
    Assertions.assertEquals("range", rangePartitionDTO.name());
    Assertions.assertEquals(PartitionDTO.Type.RANGE, rangePartitionDTO.type());
    Assertions.assertEquals(lower.dataType(), rangePartitionDTO.lower().dataType());
    Assertions.assertEquals(upper.dataType(), rangePartitionDTO.upper().dataType());
    Assertions.assertEquals(properties, rangePartitionDTO.properties());
  }

  @Test
  void testListPartitionConvert() {

    // given
    Literal<?> lower = Literals.stringLiteral(Types.StringType.get().simpleString());
    Literal<?> upper = Literals.booleanLiteral(Boolean.FALSE);

    Literal<?>[][] values = {new Literal[] {lower}, new Literal[] {upper}};

    Map<String, String> properties = Collections.singletonMap("key", "value");
    Partition identityPartition = Partitions.list("list", values, properties);

    // when
    ListPartitionDTO listPartitionDTO = (ListPartitionDTO) DTOConverters.toDTO(identityPartition);

    // then
    Assertions.assertEquals("list", listPartitionDTO.name());
    Assertions.assertEquals(PartitionDTO.Type.LIST, listPartitionDTO.type());
    Assertions.assertEquals(values.length, listPartitionDTO.lists().length);
    Assertions.assertEquals(properties, listPartitionDTO.properties());
  }

  @Test
  void testPartitionDTOConvert() {
    Literal<?> lower = Literals.stringLiteral("2008-08-08");
    Literal<?> upper = Literals.stringLiteral("us");
    Map<String, String> properties = Collections.singletonMap("key", "value");
    RangePartition rangePartition = Partitions.range("range", upper, lower, properties);
    Transform rangeTransform =
        Transforms.range(new String[] {"col1"}, new RangePartition[] {rangePartition});

    RangePartitioningDTO rangePartitioning =
        (RangePartitioningDTO) DTOConverters.toDTO(rangeTransform);
    String[] rangePartitionFieldName = rangePartitioning.fieldName();
    RangePartitionDTO[] rangePartitionAssignments = rangePartitioning.assignments();

    Assertions.assertEquals("col1", rangePartitionFieldName[0]);
    Assertions.assertEquals(lower.dataType(), rangePartitionAssignments[0].lower().dataType());
    Assertions.assertEquals("2008-08-08", rangePartitionAssignments[0].lower().value());
    Assertions.assertEquals(upper.dataType(), rangePartitionAssignments[0].upper().dataType());
    Assertions.assertEquals("us", rangePartitionAssignments[0].upper().value());
    Assertions.assertEquals(properties, rangePartitionAssignments[0].properties());

    Literal<?> value = Literals.stringLiteral(Types.StringType.get().simpleString());
    Literal<?>[][] values = {new Literal[] {value}};
    ListPartition listPartition = Partitions.list("list", values, properties);
    Transform listTransform =
        Transforms.list(new String[][] {{"col2"}}, new ListPartition[] {listPartition});
    ListPartitioningDTO listPartitioning = (ListPartitioningDTO) DTOConverters.toDTO(listTransform);
    String[][] listPartitionFieldNames = listPartitioning.fieldNames();
    ListPartitionDTO[] listPartitionAssignments = listPartitioning.assignments();

    Assertions.assertEquals("col2", listPartitionFieldNames[0][0]);
    Assertions.assertEquals(value.dataType(), listPartitionAssignments[0].lists()[0][0].dataType());
    Assertions.assertEquals(
        Types.StringType.get().simpleString(), listPartitionAssignments[0].lists()[0][0].value());
    Assertions.assertEquals(properties, listPartitionAssignments[0].properties());
  }
}
