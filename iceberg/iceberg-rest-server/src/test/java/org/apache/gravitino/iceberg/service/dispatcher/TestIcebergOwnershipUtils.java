/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service.dispatcher;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIcebergOwnershipUtils {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String USER = "test_user";
  private static final String SCHEMA_NAME = "test_schema";
  private static final String TABLE_NAME = "test_table";

  private OwnerDispatcher mockOwnerDispatcher;

  @BeforeEach
  public void setUp() {
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
  }

  @Test
  public void testSetSchemaOwner() {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    NameIdentifier expectedSchemaIdentifier =
        IcebergIdentifierUtils.toGravitinoSchemaIdentifier(METALAKE, CATALOG, namespace);
    MetadataObject expectedMetadataObject =
        NameIdentifierUtil.toMetadataObject(expectedSchemaIdentifier, Entity.EntityType.SCHEMA);

    IcebergOwnershipUtils.setSchemaOwner(METALAKE, CATALOG, namespace, USER, mockOwnerDispatcher);

    verify(mockOwnerDispatcher, times(1))
        .setOwner(eq(METALAKE), eq(expectedMetadataObject), eq(USER), eq(Owner.Type.USER));
  }

  @Test
  public void testSetTableOwner() {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
    NameIdentifier expectedTableIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, tableIdentifier);
    MetadataObject expectedMetadataObject =
        NameIdentifierUtil.toMetadataObject(expectedTableIdentifier, Entity.EntityType.TABLE);

    IcebergOwnershipUtils.setTableOwner(
        METALAKE, CATALOG, namespace, TABLE_NAME, USER, mockOwnerDispatcher);

    verify(mockOwnerDispatcher, times(1))
        .setOwner(eq(METALAKE), eq(expectedMetadataObject), eq(USER), eq(Owner.Type.USER));
  }

  @Test
  public void testSetSchemaOwnerWithNullOwnerDispatcher() {
    Namespace namespace = Namespace.of(SCHEMA_NAME);

    try {
      IcebergOwnershipUtils.setSchemaOwner(METALAKE, CATALOG, namespace, USER, null);
    } catch (Exception e) {
      fail("setSchemaOwner should handle null dispatcher gracefully, but threw: " + e.getMessage());
    }
  }

  @Test
  public void testSetTableOwnerWithNullOwnerDispatcher() {
    Namespace namespace = Namespace.of(SCHEMA_NAME);

    try {
      IcebergOwnershipUtils.setTableOwner(METALAKE, CATALOG, namespace, TABLE_NAME, USER, null);
    } catch (Exception e) {
      fail("setTableOwner should handle null dispatcher gracefully, but threw: " + e.getMessage());
    }
  }
}
