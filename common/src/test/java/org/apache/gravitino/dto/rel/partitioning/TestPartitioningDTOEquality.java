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
package org.apache.gravitino.dto.rel.partitioning;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitioningDTOEquality {

  @Test
  public void testDifferentSingleFieldStrategiesAreNotEqual() {
    // Before the fix, Hour/Month/Year DTOs lacked @EqualsAndHashCode(callSuper = true) (unlike
    // Day/Identity), so they inherited SingleFieldPartitioning's equals and different strategies
    // over the same field compared equal.
    String[] field = {"ts"};
    Assertions.assertNotEquals(HourPartitioningDTO.of(field), MonthPartitioningDTO.of(field));
    Assertions.assertNotEquals(MonthPartitioningDTO.of(field), YearPartitioningDTO.of(field));
    Assertions.assertNotEquals(HourPartitioningDTO.of(field), YearPartitioningDTO.of(field));
    Assertions.assertNotEquals(HourPartitioningDTO.of(field), DayPartitioningDTO.of(field));
    Assertions.assertNotEquals(HourPartitioningDTO.of(field), IdentityPartitioningDTO.of(field));
  }

  @Test
  public void testSameStrategyIsEqual() {
    String[] field = {"ts"};
    Assertions.assertEquals(HourPartitioningDTO.of(field), HourPartitioningDTO.of(field));
    Assertions.assertEquals(
        HourPartitioningDTO.of(field), HourPartitioningDTO.of(new String[] {"ts"}));
    Assertions.assertNotEquals(
        HourPartitioningDTO.of(field), HourPartitioningDTO.of(new String[] {"other"}));
  }

  @Test
  public void testSetSemantics() {
    Set<Partitioning> strategies =
        new HashSet<>(Arrays.asList(HourPartitioningDTO.of(new String[] {"ts"})));

    Assertions.assertFalse(strategies.contains(MonthPartitioningDTO.of(new String[] {"ts"})));
    Assertions.assertTrue(strategies.contains(HourPartitioningDTO.of(new String[] {"ts"})));
  }
}
