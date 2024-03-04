/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.util;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitionDTO;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.partitions.RangePartition;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDTOConverters {

  @Test
  void testIdentityPartitionDTOConvert() {

    // given
    String[] field1 = {"dt"};
    String[] field2 = {"country"};
    LiteralDTO literal1 =
        new LiteralDTO.Builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO literal2 =
        new LiteralDTO.Builder().withDataType(Types.StringType.get()).withValue("us").build();
    String[][] fieldNames = {field1, field2};
    LiteralDTO[] values = {literal1, literal2};

    Map<String, String> properties = Collections.singletonMap("key", "value");
    PartitionDTO identityPartitionDTO =
        IdentityPartitionDTO.builder()
            .withFieldNames(fieldNames)
            .withName("IdentityPartition")
            .withValues(values)
            .withProperties(properties)
            .build();
    // when
    com.datastrato.gravitino.rel.partitions.IdentityPartition identityPartition =
        (com.datastrato.gravitino.rel.partitions.IdentityPartition)
            DTOConverters.fromDTO(identityPartitionDTO);

    // then
    Assertions.assertTrue(Arrays.equals(fieldNames, identityPartition.fieldNames()));
    Assertions.assertEquals("IdentityPartition", identityPartition.name());
    Assertions.assertTrue(Arrays.equals(values, identityPartition.values()));
    Assertions.assertEquals(properties, identityPartition.properties());
  }

  @Test
  void testRangePartitionDTOConvert() {

    // given
    LiteralDTO lower =
        new LiteralDTO.Builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO upper =
        new LiteralDTO.Builder().withDataType(Types.StringType.get()).withValue("us").build();

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
    Assertions.assertEquals(lower, rangePartition.lower());
    Assertions.assertEquals(upper, rangePartition.upper());
    Assertions.assertEquals(properties, rangePartition.properties());
  }

  @Test
  void testListPartitionDTOConvert() {

    // given
    LiteralDTO literal1 =
        new LiteralDTO.Builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO literal2 =
        new LiteralDTO.Builder().withDataType(Types.StringType.get()).withValue("us").build();

    Map<String, String> properties = Collections.singletonMap("key", "value");
    LiteralDTO[][] literalDTOS = {new LiteralDTO[] {literal1}, new LiteralDTO[] {literal2}};
    ListPartitionDTO listPartitionDTO =
        ListPartitionDTO.builder()
            .withName("ListPartition")
            .withLists(literalDTOS)
            .withProperties(properties)
            .build();

    // when
    ListPartition listPartition = (ListPartition) DTOConverters.fromDTO(listPartitionDTO);

    // then
    Assertions.assertEquals("ListPartition", listPartition.name());
    Assertions.assertTrue(Arrays.equals(literalDTOS, listPartition.lists()));
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
}
