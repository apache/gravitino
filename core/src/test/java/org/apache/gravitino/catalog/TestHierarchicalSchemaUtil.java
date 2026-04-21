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
package org.apache.gravitino.catalog;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Tests for the core methods retained in HierarchicalSchemaUtil. The removed helpers
// (physicalNameToLevels, hasValidSegments, parentScopes) had no production callers and were
// eliminated during cleanup.

public class TestHierarchicalSchemaUtil {

  @Test
  public void testLogicalToPhysicalWithColonSeparator() {
    Assertions.assertEquals("A.B.C", HierarchicalSchemaUtil.logicalToPhysical("A:B:C", ":"));
    Assertions.assertEquals("A.B", HierarchicalSchemaUtil.logicalToPhysical("A:B", ":"));
    Assertions.assertEquals("flat", HierarchicalSchemaUtil.logicalToPhysical("flat", ":"));
  }

  @Test
  public void testLogicalToPhysicalWithCustomSeparator() {
    Assertions.assertEquals("A.B.C", HierarchicalSchemaUtil.logicalToPhysical("A/B/C", "/"));
    Assertions.assertEquals("flat", HierarchicalSchemaUtil.logicalToPhysical("flat", "/"));
  }

  @Test
  public void testPhysicalToLogicalWithColonSeparator() {
    Assertions.assertEquals("A:B:C", HierarchicalSchemaUtil.physicalToLogical("A.B.C", ":"));
    Assertions.assertEquals("A:B", HierarchicalSchemaUtil.physicalToLogical("A.B", ":"));
    Assertions.assertEquals("flat", HierarchicalSchemaUtil.physicalToLogical("flat", ":"));
  }

  @Test
  public void testRoundTripConversion() {
    String logical = "team:sales:reports";
    String separator = ":";
    String physical = HierarchicalSchemaUtil.logicalToPhysical(logical, separator);
    Assertions.assertEquals("team.sales.reports", physical);
    Assertions.assertEquals(logical, HierarchicalSchemaUtil.physicalToLogical(physical, separator));
  }

  @Test
  public void testIsNested() {
    Assertions.assertTrue(HierarchicalSchemaUtil.isNested("A:B:C", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isNested("flat", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isNested("", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isNested(null, ":"));
  }

  @Test
  public void testIsPhysicalNested() {
    Assertions.assertTrue(HierarchicalSchemaUtil.isPhysicalNested("A.B.C"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isPhysicalNested("flat"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isPhysicalNested(""));
    Assertions.assertFalse(HierarchicalSchemaUtil.isPhysicalNested(null));
  }

  @Test
  public void testFilterDirectChildrenTopLevel() {
    List<String> allNames = Arrays.asList("A", "A.B", "A.B.C", "B");
    List<String> topLevel = HierarchicalSchemaUtil.filterDirectChildren(allNames, null);
    Assertions.assertEquals(2, topLevel.size());
    Assertions.assertTrue(topLevel.contains("A"));
    Assertions.assertTrue(topLevel.contains("B"));
  }

  @Test
  public void testFilterDirectChildrenUnderParent() {
    List<String> allNames = Arrays.asList("A", "A.B", "A.C", "A.B.D", "B");
    List<String> children = HierarchicalSchemaUtil.filterDirectChildren(allNames, "A");
    Assertions.assertEquals(2, children.size());
    Assertions.assertTrue(children.contains("A.B"));
    Assertions.assertTrue(children.contains("A.C"));
    Assertions.assertFalse(children.contains("A.B.D"));
  }

  @Test
  public void testFilterDirectChildrenDeepNesting() {
    List<String> allNames = Arrays.asList("A", "A.B", "A.B.C", "A.B.D", "A.B.C.E");
    List<String> children = HierarchicalSchemaUtil.filterDirectChildren(allNames, "A.B");
    Assertions.assertEquals(2, children.size());
    Assertions.assertTrue(children.contains("A.B.C"));
    Assertions.assertTrue(children.contains("A.B.D"));
    Assertions.assertFalse(children.contains("A.B.C.E"));
  }

  @Test
  public void testFilterDirectChildrenEmptyParent() {
    List<String> allNames = Arrays.asList("A", "B", "A.B");
    List<String> topLevel = HierarchicalSchemaUtil.filterDirectChildren(allNames, "");
    Assertions.assertEquals(2, topLevel.size());
    Assertions.assertTrue(topLevel.contains("A"));
    Assertions.assertTrue(topLevel.contains("B"));
  }
}
