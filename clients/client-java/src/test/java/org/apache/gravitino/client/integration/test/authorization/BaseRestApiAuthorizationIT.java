/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class BaseRestApiAuthorizationIT extends BaseIT {

  protected static final String METALAKE = "testMetalake";

  protected static final String USER = "tester";

  protected static final String NORMAL_USER = "tester2";

  /** Mock a normal user without permissions. */
  protected static GravitinoAdminClient normalUserClient;

  private static final Logger LOG = LoggerFactory.getLogger(BaseRestApiAuthorizationIT.class);

  /** Mock a test staging directory for job test. */
  protected static File testStagingDir;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.CACHE_ENABLED.getKey(),
            "false",
            Configs.AUTHENTICATORS.getKey(),
            "simple"));
    putServiceAdmin();
    super.startIntegrationTest();
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(NORMAL_USER);
    normalUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(NORMAL_USER).build();
  }

  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), USER);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.dropMetalake(METALAKE, true);

    if (normalUserClient != null) {
      normalUserClient.close();
      normalUserClient = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
    super.stopIntegrationTest();
  }


  /**
   * Generate a test entry script for job operation test
   *
   * @return the path of the entry script
   */
  protected static String generateTestEntryScript() {
    String content =
            "#!/bin/bash\n"
                    + "echo \"starting test job\"\n\n"
                    + "bin=\"$(dirname \"${BASH_SOURCE-$0}\")\"\n"
                    + "bin=\"$(cd \"${bin}\">/dev/null; pwd)\"\n\n"
                    + ". \"${bin}/common.sh\"\n\n"
                    + "sleep 3\n\n"
                    + "JOB_NAME=\"test_job-$(date +%s)-$1\"\n\n"
                    + "echo \"Submitting job with name: $JOB_NAME\"\n\n"
                    + "echo \"$1\"\n\n"
                    + "echo \"$2\"\n\n"
                    + "echo \"$ENV_VAR\"\n\n"
                    + "if [[ \"$2\" == \"success\" ]]; then\n"
                    + "  exit 0\n"
                    + "elif [[ \"$2\" == \"fail\" ]]; then\n"
                    + "  exit 1\n"
                    + "else\n"
                    + "  exit 2\n"
                    + "fi\n";

    try {
      File scriptFile = new File(testStagingDir, "test-job.sh");
      Files.writeString(scriptFile.toPath(), content);
      if (!scriptFile.setExecutable(true)) {
        throw new RuntimeException("Failed to set script as executable");
      }
      return scriptFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test entry script", e);
    }
  }

  /**
   * Generate a test lib script for job operation test
   *
   * @return the path of the lib script
   */
  protected static String generateTestLibScript() {
    String content = "#!/bin/bash\necho \"in common script\"\n";

    try {
      File scriptFile = new File(testStagingDir, "common.sh");
      Files.writeString(scriptFile.toPath(), content);
      if (!scriptFile.setExecutable(true)) {
        throw new RuntimeException("Failed to set script as executable");
      }
      return scriptFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test lib script", e);
    }
  }
}
