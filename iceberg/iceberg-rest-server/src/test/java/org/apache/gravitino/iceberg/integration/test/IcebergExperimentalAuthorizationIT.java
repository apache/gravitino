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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.UUID;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for Spark authorization through the experimental Iceberg REST service.
 *
 * <p>The inherited tests exercise table creation, loading, updates, deletion, listing, renaming,
 * nested namespaces, and table privilege enforcement against the experimental Iceberg artifacts.
 */
@Tag("gravitino-docker-test")
public class IcebergExperimentalAuthorizationIT extends IcebergTableAuthorizationIT {

  private static final String EXPERIMENTAL_SERVICE_NAME = "iceberg-rest-experimental";
  private static final String EXPERIMENTAL_SERVER_DIRECTORY = "iceberg-rest-experimental-server";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    String rootDirectory = System.getenv("GRAVITINO_ROOT_DIR");
    String experimentalServerHome =
        Paths.get(rootDirectory, "iceberg", EXPERIMENTAL_SERVER_DIRECTORY).toString();
    customConfigs.put(
        "gravitino." + EXPERIMENTAL_SERVICE_NAME + ".classpath",
        String.join(
            ",",
            Paths.get(experimentalServerHome, "build", "libs").toString(),
            Paths.get(experimentalServerHome, "build", "dependencies").toString(),
            Paths.get(rootDirectory, "iceberg", "iceberg-rest-server", "src", "main", "resources")
                .toString()));
    super.startIntegrationTest();
  }

  @Override
  protected String getIcebergAuxServiceName() {
    return EXPERIMENTAL_SERVICE_NAME;
  }

  @Override
  protected TestDatabaseName getTestDatabaseName() {
    return TestDatabaseName.PG_ICEBERG_EXPERIMENTAL_AUTHZ_IT;
  }

  @Test
  void testSparkCreateNamespaceAuthorization() {
    String namespace = "experimental_authz_ns";
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();

    grantCatalogPrivileges(Privileges.UseCatalog.allow());
    sql("USE %s", SPARK_CATALOG_NAME);

    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("CREATE DATABASE %s", namespace));

    grantCatalogPrivileges(Privileges.CreateSchema.allow());
    Assertions.assertDoesNotThrow(() -> sql("CREATE DATABASE %s", namespace));
    Assertions.assertDoesNotThrow(() -> sql("USE %s", namespace));
  }

  private void grantCatalogPrivileges(Privilege... privileges) {
    String roleName = "experimentalCatalogRole_" + UUID.randomUUID();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(GRAVITINO_CATALOG_NAME, ImmutableList.copyOf(privileges));
    metalakeClientWithAllPrivilege.createRole(
        roleName, new HashMap<>(), ImmutableList.of(catalogObject));
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }
}
