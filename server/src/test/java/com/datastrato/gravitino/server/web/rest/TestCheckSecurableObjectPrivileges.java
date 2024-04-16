/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.lock.LockManager;
import com.google.common.collect.Lists;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCheckSecurableObjectPrivileges {

  private static final CatalogManager catalogManager = mock(CatalogManager.class);
  private static final Catalog catalog = mock(Catalog.class);

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
  }

  @Test
  void testCatalogPrivileges() {
    RoleOperations operations = new RoleOperations(catalogManager);

    // catalog1 supports catalog operations except for listing.
    RoleCreateRequest request1 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadCatalog.get().name().toString(),
                Privileges.AlterCatalog.get().name().toString(),
                Privileges.CreateCatalog.get().name().toString(),
                Privileges.DropCatalog.get().name().toString()),
            SecurableObjects.ofCatalog("catalog").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request1));

    // * supports list catalogs
    RoleCreateRequest request2 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListCatalog.get().name().toString()),
            SecurableObjects.ofAllCatalogs().toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request2));

    // catalog1 supports schema operations
    RoleCreateRequest request3 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadSchema.get().name().toString(),
                Privileges.AlterSchema.get().name().toString(),
                Privileges.CreateSchema.get().name().toString(),
                Privileges.DropSchema.get().name().toString()),
            SecurableObjects.ofCatalog("catalog1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request3));

    // catalog1 doesn't support list catalogs
    RoleCreateRequest request4 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListCatalog.get().name().toString()),
            SecurableObjects.ofCatalog("catalog1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request4));

    // * doesn't support schema operations
    RoleCreateRequest request5 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListSchema.get().name().toString()),
            SecurableObjects.ofAllCatalogs().toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request5));
  }

  @Test
  void testSchemaPrivileges() {
    RoleOperations operations = new RoleOperations(catalogManager);
    when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog1");

    // schema1 supports catalog operations except for listing.
    RoleCreateRequest request1 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadSchema.get().name().toString(),
                Privileges.AlterSchema.get().name().toString(),
                Privileges.CreateSchema.get().name().toString(),
                Privileges.DropSchema.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request1));

    // schema1 doesn't support listing
    RoleCreateRequest request2 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListSchema.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "table1").toString());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request2));

    // Relational schema supports table
    RoleCreateRequest request3 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.ListTable.get().name().toString(),
                Privileges.LoadTable.get().name().toString(),
                Privileges.AlterTable.get().name().toString(),
                Privileges.CreateTable.get().name().toString(),
                Privileges.DropTable.get().name().toString(),
                Privileges.ReadTable.get().name().toString(),
                Privileges.WriteTable.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request3));

    // Relational schema doesn't support topic and fileset
    RoleCreateRequest request4 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadTopic.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request4));

    RoleCreateRequest request5 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadFileset.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request5));

    when(catalog.type()).thenReturn(Catalog.Type.FILESET);

    // Fileset schema supports fileset
    RoleCreateRequest request6 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.ListFileset.get().name().toString(),
                Privileges.LoadFileset.get().name().toString(),
                Privileges.AlterFileset.get().name().toString(),
                Privileges.CreateFileset.get().name().toString(),
                Privileges.DropFileset.get().name().toString(),
                Privileges.ReadFileset.get().name().toString(),
                Privileges.WriteFileset.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request6));

    // Fileset schema doesn't support table and topic
    RoleCreateRequest request7 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadTable.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request7));

    RoleCreateRequest request8 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadTopic.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request8));

    when(catalog.type()).thenReturn(Catalog.Type.MESSAGING);

    // Messaging schema supports topic
    RoleCreateRequest request9 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.ListTopic.get().name().toString(),
                Privileges.LoadTopic.get().name().toString(),
                Privileges.AlterTopic.get().name().toString(),
                Privileges.CreateTopic.get().name().toString(),
                Privileges.DropTopic.get().name().toString(),
                Privileges.ReadTopic.get().name().toString(),
                Privileges.WriteTopic.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request9));

    // Messaging schema doesn't support table and fileset
    RoleCreateRequest request10 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadTable.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request10));

    RoleCreateRequest request11 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.LoadFileset.get().name().toString()),
            SecurableObjects.ofSchema(catalogObject, "schema1").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request11));
  }

  @Test
  void testTablePrivileges() {
    RoleOperations operations = new RoleOperations(catalogManager);
    when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);

    SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog1");
    SecurableObject schemaObject = SecurableObjects.ofSchema(catalogObject, "schema1");

    // table1 supports catalog operations except for listing.
    RoleCreateRequest request1 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadTable.get().name().toString(),
                Privileges.AlterTable.get().name().toString(),
                Privileges.CreateTable.get().name().toString(),
                Privileges.DropTable.get().name().toString(),
                Privileges.ReadTable.get().name().toString(),
                Privileges.WriteTable.get().name().toString()),
            SecurableObjects.ofFileset(schemaObject, "table1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request1));

    // table1 doesn't support listing
    RoleCreateRequest request2 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListTable.get().name().toString()),
            SecurableObjects.ofTable(schemaObject, "table1").toString());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request2));
  }

  @Test
  void testTopicPrivileges() {
    RoleOperations operations = new RoleOperations(catalogManager);
    when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.MESSAGING);

    SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog1");
    SecurableObject schemaObject = SecurableObjects.ofSchema(catalogObject, "schema1");

    // topic1 supports catalog operations except for listing.
    RoleCreateRequest request1 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadTopic.get().name().toString(),
                Privileges.AlterTopic.get().name().toString(),
                Privileges.CreateTopic.get().name().toString(),
                Privileges.DropTopic.get().name().toString(),
                Privileges.ReadTopic.get().name().toString(),
                Privileges.WriteTopic.get().name().toString()),
            SecurableObjects.ofTopic(schemaObject, "topic1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request1));

    // topic1 doesn't support listing
    RoleCreateRequest request2 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListTopic.get().name().toString()),
            SecurableObjects.ofTopic(schemaObject, "topic1").toString());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request2));
  }

  @Test
  void testFilesetPrivileges() {
    RoleOperations operations = new RoleOperations(catalogManager);
    when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.FILESET);

    SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog1");
    SecurableObject schemaObject = SecurableObjects.ofSchema(catalogObject, "schema1");

    // fileset1 supports catalog operations except for listing.
    RoleCreateRequest request1 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(
                Privileges.LoadFileset.get().name().toString(),
                Privileges.AlterFileset.get().name().toString(),
                Privileges.CreateFileset.get().name().toString(),
                Privileges.DropFileset.get().name().toString(),
                Privileges.ReadFileset.get().name().toString(),
                Privileges.WriteFileset.get().name().toString()),
            SecurableObjects.ofFileset(schemaObject, "fileset1").toString());
    Assertions.assertDoesNotThrow(
        () -> operations.checkSecurableObjectPrivileges("metalake", request1));

    // fileset1 doesn't support listing
    RoleCreateRequest request2 =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            Lists.newArrayList(Privileges.ListFileset.get().name().toString()),
            SecurableObjects.ofFileset(schemaObject, "fileset1").toString());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.checkSecurableObjectPrivileges("metalake", request2));
  }
}
