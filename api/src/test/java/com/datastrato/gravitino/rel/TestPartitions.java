/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.IdentityPartition;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.partitions.RangePartition;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitions {

  @Test
  public void testPartitions() {
    Partition partition =
        Partitions.range("p0", Literals.NULL, Literals.integerLiteral(6), Maps.newHashMap());
    Assertions.assertEquals("p0", partition.name());
    Assertions.assertEquals(Maps.newHashMap(), partition.properties());
    Assertions.assertEquals(Literals.NULL, ((RangePartition) partition).upper());
    Assertions.assertEquals(Literals.integerLiteral(6), ((RangePartition) partition).lower());

    partition =
        Partitions.list(
            "p202204_California",
            new Literal[][] {
              {
                Literals.dateLiteral(LocalDate.parse("2022-04-01")),
                Literals.stringLiteral("Los Angeles")
              },
              {
                Literals.dateLiteral(LocalDate.parse("2022-04-01")),
                Literals.stringLiteral("San Francisco")
              }
            },
            Maps.newHashMap());
    Assertions.assertEquals("p202204_California", partition.name());
    Assertions.assertEquals(Maps.newHashMap(), partition.properties());
    Assertions.assertEquals(
        Literals.dateLiteral(LocalDate.parse("2022-04-01")),
        ((ListPartition) partition).lists()[0][0]);
    Assertions.assertEquals(
        Literals.stringLiteral("Los Angeles"), ((ListPartition) partition).lists()[0][1]);
    Assertions.assertEquals(
        Literals.dateLiteral(LocalDate.parse("2022-04-01")),
        ((ListPartition) partition).lists()[1][0]);
    Assertions.assertEquals(
        Literals.stringLiteral("San Francisco"), ((ListPartition) partition).lists()[1][1]);

    partition =
        Partitions.identity(
            "dt=2008-08-08/country=us",
            new String[][] {{"dt"}, {"country"}},
            new Literal[] {
              Literals.dateLiteral(LocalDate.parse("2008-08-08")), Literals.stringLiteral("us")
            },
            ImmutableMap.of("location", "/user/hive/warehouse/tpch_flat_orc_2.db/orders"));
    Assertions.assertEquals("dt=2008-08-08/country=us", partition.name());
    Assertions.assertEquals(
        ImmutableMap.of("location", "/user/hive/warehouse/tpch_flat_orc_2.db/orders"),
        partition.properties());
    Assertions.assertArrayEquals(
        new String[] {"dt"}, ((IdentityPartition) partition).fieldNames()[0]);
    Assertions.assertArrayEquals(
        new String[] {"country"}, ((IdentityPartition) partition).fieldNames()[1]);
    Assertions.assertEquals(
        Literals.dateLiteral(LocalDate.parse("2008-08-08")),
        ((IdentityPartition) partition).values()[0]);
    Assertions.assertEquals(
        Literals.stringLiteral("us"), ((IdentityPartition) partition).values()[1]);
  }
}
