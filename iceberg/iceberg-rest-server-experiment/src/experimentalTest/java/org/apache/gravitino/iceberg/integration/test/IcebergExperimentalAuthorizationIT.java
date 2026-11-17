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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.iceberg.IcebergBuild;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;

/**
 * Integration test for Spark authorization through the Iceberg REST service loaded from the
 * experimental classpath.
 *
 * <p>The inherited tests exercise table creation, loading, updates, deletion, listing, renaming,
 * nested namespaces, and table privilege enforcement against the experimental Iceberg artifacts.
 */
@Tag("gravitino-docker-test")
public class IcebergExperimentalAuthorizationIT extends IcebergTableAuthorizationIT {

  private static final String EXPERIMENTAL_SERVER_DIRECTORY = "iceberg-rest-server-experiment";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Assertions.assertEquals(
        System.getProperty("datastratoIcebergGitCommit"),
        IcebergBuild.gitCommitId(),
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

    Path serverLog = getServerLog();
    long logOffset = Files.exists(serverLog) ? Files.size(serverLog) : 0;
    super.startIntegrationTest();

    String startupLog = readLogFrom(serverLog, logOffset);
    Assertions.assertTrue(
        startupLog.contains(System.getProperty("datastratoIcebergGitCommit")),
        "The Iceberg REST service must load the Datastrato Iceberg runtime");
    if (ITUtils.DEPLOY_TEST_MODE.equals(System.getProperty(ITUtils.TEST_MODE))) {
      Assertions.assertTrue(
          startupLog.contains(EXPERIMENTAL_SERVER_DIRECTORY),
          "The deployed Iceberg REST service must load its configured experimental classpath");
    }
  }

  @Override
  protected TestDatabaseName getTestDatabaseName() {
    return TestDatabaseName.PG_ICEBERG_EXPERIMENTAL_AUTHZ_IT;
  }

  /**
   * Spark 4.1 wraps a missing table in a subclass of {@link AnalysisException}, unlike Spark 3.4.
   *
   * @param executable the table load operation
   */
  @Override
  protected void assertNoSuchTableException(Executable executable) {
    Assertions.assertThrows(AnalysisException.class, executable);
  }

  private Path getServerLog() {
    if (ITUtils.DEPLOY_TEST_MODE.equals(System.getProperty(ITUtils.TEST_MODE))) {
      return Paths.get(System.getenv("GRAVITINO_HOME"), "logs", "gravitino-server.log");
    }
    return Paths.get(System.getProperty("gravitino.log.path"));
  }

  private String readLogFrom(Path logFile, long offset) throws IOException {
    byte[] logContent = Files.readAllBytes(logFile);
    int start = logContent.length >= offset ? Math.toIntExact(offset) : 0;
    return new String(logContent, start, logContent.length - start, StandardCharsets.UTF_8);
  }
}
