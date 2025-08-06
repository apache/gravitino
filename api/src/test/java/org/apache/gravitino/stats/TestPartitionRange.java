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
package org.apache.gravitino.stats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionRange {

  @Test
  public void testPartitionRange() {
    PartitionRange range1 = PartitionRange.lessThan("upper");
    Assertions.assertTrue(range1.upperPartitionName.isPresent());
    Assertions.assertFalse(range1.lowerPartitionName.isPresent());
    Assertions.assertEquals("upper", range1.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range1.comparator().type());

    PartitionRange range2 = PartitionRange.greaterOrEqual("lower");
    Assertions.assertFalse(range2.upperPartitionName.isPresent());
    Assertions.assertTrue(range2.lowerPartitionName.isPresent());
    Assertions.assertEquals("lower", range2.lowerPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range2.comparator().type());

    PartitionRange range3 = PartitionRange.greaterOrEqual("lower", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range3.lowerPartitionName.isPresent());
    Assertions.assertFalse(range3.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range3.lowerPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range3.comparator().type());

    PartitionRange range4 = PartitionRange.lessThan("upper", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range4.upperPartitionName.isPresent());
    Assertions.assertFalse(range4.lowerPartitionName.isPresent());
    Assertions.assertEquals("upper", range4.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range4.comparator().type());

    PartitionRange range5 = PartitionRange.between("lower", "upper", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range5.lowerPartitionName.isPresent());
    Assertions.assertTrue(range5.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range5.lowerPartitionName.get());
    Assertions.assertEquals("upper", range5.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range5.comparator().type());

    PartitionRange range6 = PartitionRange.between("lower", "upper");
    Assertions.assertTrue(range6.lowerPartitionName.isPresent());
    Assertions.assertTrue(range6.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range6.lowerPartitionName.get());
    Assertions.assertEquals("upper", range6.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range6.comparator().type());
  }
}
