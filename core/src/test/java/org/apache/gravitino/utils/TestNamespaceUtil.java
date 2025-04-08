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

import com.google.common.base.Joiner;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespaceUtil {

  @Test
  public void testCheckNamespace() {
    Namespace a = Namespace.of("a");
    Namespace ab = Namespace.of("a", "b");
    Namespace abcd = Namespace.of("a", "b", "c", "d");

    // Test metalake
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> NamespaceUtil.checkMetalake(null));
    Throwable excep =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkMetalake(ab));
    Assertions.assertTrue(
        excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // Test catalog
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> NamespaceUtil.checkCatalog(null));
    Throwable excep1 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkCatalog(ab));
    Assertions.assertTrue(
        excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // Test schema
    Assertions.assertThrows(IllegalNamespaceException.class, () -> NamespaceUtil.checkSchema(null));
    Throwable excep2 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkSchema(a));
    Assertions.assertTrue(
        excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // Test table
    Assertions.assertThrows(IllegalNamespaceException.class, () -> NamespaceUtil.checkTable(null));
    Throwable excep3 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkTable(abcd));
    Assertions.assertTrue(
        excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));

    // Test model
    Assertions.assertThrows(IllegalNamespaceException.class, () -> NamespaceUtil.checkModel(null));
    Throwable excep4 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkModel(abcd));
    Assertions.assertTrue(
        excep4.getMessage().contains("Model namespace must be non-null and have 3 levels"));

    // Test model version
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> NamespaceUtil.checkModelVersion(null));
    Throwable excep5 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkModelVersion(ab));
    Assertions.assertTrue(
        excep5.getMessage().contains("Model version namespace must be non-null and have 4 levels"));
  }

  @Test
  void testOfUser() {
    String metalake = "metalake";
    Assertions.assertEquals(
        Joiner.on(".").join(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME),
        NamespaceUtil.ofUser(metalake).toString());

    Assertions.assertThrows(IllegalArgumentException.class, () -> NamespaceUtil.ofUser(null));
  }

  @Test
  void testOfGroup() {
    String metalake = "metalake";
    Namespace namespace = NamespaceUtil.ofGroup(metalake);

    Assertions.assertEquals(
        Joiner.on(".")
            .join(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME),
        namespace.toString());
    Assertions.assertThrows(IllegalArgumentException.class, () -> NamespaceUtil.ofGroup(null));
  }

  @Test
  void testOfRole() {
    String metalake = "metalake";
    Namespace namespace = NamespaceUtil.ofRole(metalake);

    Assertions.assertEquals(
        Joiner.on(".").join(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME),
        namespace.toString());
    Assertions.assertThrows(IllegalArgumentException.class, () -> NamespaceUtil.ofRole(null));
  }
}
