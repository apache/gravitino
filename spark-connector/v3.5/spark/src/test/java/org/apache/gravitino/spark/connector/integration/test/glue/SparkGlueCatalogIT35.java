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
package org.apache.gravitino.spark.connector.integration.test.glue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark35;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

/**
 * Integration test for Spark 3.5 Glue catalog connector. Starts LocalStack to mock AWS Glue API and
 * S3 storage.
 */
public class SparkGlueCatalogIT35 extends SparkGlueCatalogIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static GravitinoLocalStackContainer localStack;

  @BeforeAll
  protected void startUp() throws Exception {
    containerSuite.startLocalStackContainer();
    localStack = containerSuite.getLocalStackContainer();

    // Wait for LocalStack to be ready
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result =
                    localStack.executeInContainer(
                        "awslocal", "iam", "create-user", "--user-name", "test-user");
                return result.getExitCode() == 0;
              } catch (Exception e) {
                return false;
              }
            });

    // Create S3 bucket for test data
    localStack.executeInContainer("awslocal", "s3", "mb", "s3://" + S3_BUCKET_NAME);

    // Set Glue endpoint (same host:port as S3 in LocalStack)
    String glueEndpoint =
        String.format(
            "http://%s:%d", localStack.getContainerIpAddress(), GravitinoLocalStackContainer.PORT);
    setGlueEndpoint(glueEndpoint);
    setAwsCredentials("test", "test");

    // Set S3 credentials for Spark S3A filesystem
    setS3Credentials(glueEndpoint, "test", "test");

    super.startUp();
  }

  @AfterAll
  protected void stop() throws IOException, InterruptedException {
    try {
      super.stop();
    } finally {
      if (localStack != null) {
        localStack.close();
      }
    }
  }

  @Test
  void testCatalogClassName() {
    String catalogClass =
        getSparkSession()
            .sessionState()
            .conf()
            .getConfString("spark.sql.catalog." + getCatalogName());
    Assertions.assertEquals(GravitinoGlueCatalogSpark35.class.getName(), catalogClass);
  }
}
