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
package org.apache.gravitino.utils;

import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHierarchicalSchemaUtil {

  private static final String PHYS = "\u0001";

  @Test
  public void testPhysicalSeparator() {
    Assertions.assertEquals(PHYS, HierarchicalSchemaUtil.physicalSeparator());
  }

  @Test
  public void testLogicalToPhysicalAndBack() {
    String logical = "A:B:C";
    String sep = ":";
    String physical = HierarchicalSchemaUtil.logicalToPhysical(logical, sep);
    Assertions.assertEquals("A" + PHYS + "B" + PHYS + "C", physical);
    Assertions.assertEquals(logical, HierarchicalSchemaUtil.physicalToLogical(physical, sep));
  }

  @Test
  public void testLogicalToPhysicalMultiCharSeparator() {
    String logical = "ns|sub|leaf";
    String physical = HierarchicalSchemaUtil.logicalToPhysical(logical, "|");
    Assertions.assertEquals("ns" + PHYS + "sub" + PHYS + "leaf", physical);
    Assertions.assertEquals(logical, HierarchicalSchemaUtil.physicalToLogical(physical, "|"));
  }

  @Test
  public void testLogicalToPhysicalSingleSegment() {
    Assertions.assertEquals("sales", HierarchicalSchemaUtil.logicalToPhysical("sales", ":"));
  }

  @Test
  public void testLogicalToPhysicalBlankArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.logicalToPhysical("", ":"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.logicalToPhysical("A", ""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.logicalToPhysical("A", "   "));
  }

  @Test
  public void testPhysicalToLogicalBlankArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.physicalToLogical("", ":"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> HierarchicalSchemaUtil.physicalToLogical("A" + PHYS + "B", ""));
  }

  @Test
  public void testIsHierarchical() {
    Assertions.assertTrue(HierarchicalSchemaUtil.isHierarchical("A:B", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isHierarchical("AB", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isHierarchical("A", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isHierarchical("", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isHierarchical("   ", ":"));
    Assertions.assertFalse(HierarchicalSchemaUtil.isHierarchical(null, ":"));
  }

  @Test
  public void testGetAncestorNames() {
    Assertions.assertEquals(
        List.of("A", "A:B"), HierarchicalSchemaUtil.getAncestorNames("A:B:C", ":"));
    Assertions.assertEquals(List.of(), HierarchicalSchemaUtil.getAncestorNames("root", ":"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.getAncestorNames("", ":"));
  }

  @Test
  public void testAllScopes() {
    Assertions.assertEquals(
        List.of("A:B:C", "A:B", "A"), HierarchicalSchemaUtil.allScopes("A:B:C", ":"));
    Assertions.assertEquals(List.of("root"), HierarchicalSchemaUtil.allScopes("root", ":"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> HierarchicalSchemaUtil.allScopes("", ":"));
  }

  @Test
  public void testSchemaSeparatorWhenConfigNull() throws IllegalAccessException {
    Config saved = GravitinoEnv.getInstance().config();
    try {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
      Assertions.assertEquals(":", HierarchicalSchemaUtil.schemaSeparator());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", saved, true);
    }
  }

  @Test
  public void testSchemaSeparatorFromConfig() throws IllegalAccessException {
    Config saved = GravitinoEnv.getInstance().config();
    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_SEPARATOR)).thenReturn("|");
    try {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
      Assertions.assertEquals("|", HierarchicalSchemaUtil.schemaSeparator());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", saved, true);
    }
  }

  @Test
  public void testSchemaSeparatorBlankFallsBackToDefault() throws IllegalAccessException {
    Config saved = GravitinoEnv.getInstance().config();
    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_SEPARATOR)).thenReturn("  ");
    try {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
      Assertions.assertEquals(":", HierarchicalSchemaUtil.schemaSeparator());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", saved, true);
    }
  }
}
