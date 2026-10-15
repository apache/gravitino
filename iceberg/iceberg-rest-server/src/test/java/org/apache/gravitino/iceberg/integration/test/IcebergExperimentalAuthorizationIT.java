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

import java.nio.file.Paths;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.iceberg.IcebergBuild;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * Integration test for Spark authorization through the Iceberg REST service loaded from the
 * experimental classpath.
 *
 * <p>The inherited tests exercise table creation, loading, updates, deletion, listing, renaming,
 * nested namespaces, and table privilege enforcement against the experimental Iceberg artifacts.
 */
@Tag("gravitino-docker-test")
public class IcebergExperimentalAuthorizationIT extends IcebergTableAuthorizationIT {

  private static final String EXPERIMENTAL_SERVER_DIRECTORY = "iceberg-rest-experimental-server";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Assertions.assertEquals(
        System.getProperty("datastratoIcebergVersion"),
        IcebergBuild.version(),
        "The experimental integration test must run with the Datastrato Iceberg runtime");

    String rootDirectory = System.getenv("GRAVITINO_ROOT_DIR");
    String experimentalServerHome =
        Paths.get(rootDirectory, "iceberg", EXPERIMENTAL_SERVER_DIRECTORY).toString();
    customConfigs.put(
        "gravitino.iceberg-rest.classpath",
        String.join(
            ",",
            Paths.get(experimentalServerHome, "build", "libs").toString(),
            Paths.get(experimentalServerHome, "build", "dependencies").toString()));
    super.startIntegrationTest();
  }

  @Override
  protected TestDatabaseName getTestDatabaseName() {
    return TestDatabaseName.PG_ICEBERG_EXPERIMENTAL_AUTHZ_IT;
  }
}
