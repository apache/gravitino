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
package org.apache.gravitino.integration.test.util;

import java.util.Set;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an extension for JUnit 5, which aims to perform certain operations (such as resource
 * recycling, etc.) after all test executions are completed (regardless of success or failure). You
 * can Refer to {@link AbstractIT} for more information.
 */
public class EnableIfDockerTestExtension implements BeforeAllCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnableIfDockerTestExtension.class);
  String DOCKER_TEST_TAG = "gravitino-docker-test";

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    Set<String> tags = extensionContext.getTags();
    if (!tags.contains(DOCKER_TEST_TAG)) {
      // Only check if the test is having tagged with `gravitino-docker-test`
      return;
    }

    final String dockerTest = String.format("%s:dockerTest", DOCKER_TEST_TAG);
    boolean isDockerTest =
        System.getProperty(dockerTest) == null
            ? false
            : System.getProperty(dockerTest).equalsIgnoreCase("true");
    if (!isDockerTest) {
      final String dockerRunning = String.format("%s:dockerRunning", DOCKER_TEST_TAG);
      final String macDockerConnector = String.format("%s:macDockerConnector", DOCKER_TEST_TAG);
      final String isOrbStack = String.format("%s:isOrbStack", DOCKER_TEST_TAG);

      LOGGER.warn("------------------ Check Docker environment ---------------------");
      LOGGER.warn(
          String.format(
              "Docker server status ............................................ [%s]",
              System.getProperty(dockerRunning)));
      if (System.getProperty("os.name").toLowerCase().contains("mac")) {
        LOGGER.warn(
            String.format(
                "mac-docker-connector status ..................................... [%s]",
                System.getProperty(macDockerConnector)));
        LOGGER.warn(
            String.format(
                "OrbStack status ................................................. [%s]",
                System.getProperty(isOrbStack)));
      }
      LOGGER.warn(
          String.format(
              "Run test cases without `gravitino-docker-test` tag .............. [%s test]",
              System.getProperty("testMode")));
      LOGGER.warn("-----------------------------------------------------------------");
      LOGGER.warn(
          "Tip: Please make sure to use `OrbStack` or execute the `dev/docker/tools/mac-docker-connector.sh` script before running the integration test on macOS.");

      throw new TestAbortedException(
          "Disable running Gravitino docker integration tests without Docker environment.");
    }
  }
}
