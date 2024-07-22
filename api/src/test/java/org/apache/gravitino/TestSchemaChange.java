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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.SchemaChange.RemoveProperty;
import org.apache.gravitino.SchemaChange.SetProperty;
import org.junit.jupiter.api.Test;

public class TestSchemaChange {

  @Test
  void testSetProperty() {
    String property = "Jam";
    String value = "Strawberry";
    SetProperty change = (SetProperty) SchemaChange.setProperty(property, value);

    assertEquals(property, change.getProperty());
    assertEquals(value, change.getValue());
  }

  @Test
  void testRemoveProperty() {
    String property = "Jam";
    RemoveProperty change = (RemoveProperty) SchemaChange.removeProperty(property);

    assertEquals(property, change.getProperty());
  }

  @Test
  void testRemovePropertyEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) SchemaChange.removeProperty(propertyA);
    String propertyB = "property A";
    RemoveProperty changeB = (RemoveProperty) SchemaChange.removeProperty(propertyB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testRemovePropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) SchemaChange.removeProperty(propertyA);
    String propertyB = "property B";
    RemoveProperty changeB = (RemoveProperty) SchemaChange.removeProperty(propertyB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) SchemaChange.setProperty(propertyA, valueA);
    String propertyB = "property A";
    String valueB = "A";
    SetProperty changeB = (SetProperty) SchemaChange.setProperty(propertyB, valueB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) SchemaChange.setProperty(propertyA, valueA);
    String propertyB = "property B";
    String valueB = "B";
    SetProperty changeB = (SetProperty) SchemaChange.setProperty(propertyB, valueB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }
}
