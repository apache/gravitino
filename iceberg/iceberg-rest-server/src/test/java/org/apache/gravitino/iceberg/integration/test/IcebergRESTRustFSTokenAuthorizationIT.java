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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.RustFSContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Runs the cloud credential vending tests against RustFS. RustFS enforces the session policy
 * attached to an {@code AssumeRole} request, so the read-only downgrade a caller receives without
 * {@code MODIFY_TABLE} is observable without a cloud account.
 */
@Tag("gravitino-docker-test")
public class IcebergRESTRustFSTokenAuthorizationIT
    extends IcebergRESTCloudTokenAuthorizationBaseIT {

  private static final String BUCKET_NAME = "gravitino-rustfs-it";

  private static final String BUNDLE_NAME = "iceberg-aws-bundle";

  private final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private String s3Endpoint;
  private String warehouse;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startRustFSContainer();
    RustFSContainer rustFSContainer = containerSuite.getRustFSContainer();
    rustFSContainer.createBucket(BUCKET_NAME);
    this.s3Endpoint = rustFSContainer.getS3Endpoint();
    this.warehouse = String.format("s3://%s/test1", BUCKET_NAME);

    // In deploy mode the server resolves S3FileIO from its own classpath, so the bundle has to be
    // in place before it starts.
    setupCloudBundles();

    super.startIntegrationTest();

    createSchemaIfAbsent();
  }

  @Override
  public Map<String, String> getCustomProperties() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);
    configMap.put(S3Properties.GRAVITINO_S3_REGION, RustFSContainer.REGION);
    configMap.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, RustFSContainer.ACCESS_KEY);
    configMap.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, RustFSContainer.SECRET_KEY);
    configMap.put(S3Properties.GRAVITINO_S3_ROLE_ARN, RustFSContainer.ROLE_ARN);
    configMap.put(S3Properties.GRAVITINO_S3_ENDPOINT, s3Endpoint);
    configMap.put(S3Properties.GRAVITINO_S3_STS_ENDPOINT, s3Endpoint);
    // RustFS serves buckets as a path segment rather than a subdomain.
    configMap.put(S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, "true");
    configMap.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    configMap.put(IcebergConstants.WAREHOUSE, warehouse);
    return configMap;
  }

  @Override
  protected void assertStorageWriteDenied(Executable operation) {
    SparkException error = Assertions.assertThrows(SparkException.class, operation);
    Throwable cause = error;
    while (cause != null && !(cause instanceof S3Exception)) {
      cause = cause.getCause();
    }
    Assertions.assertNotNull(
        cause, "Expected an S3 authorization failure in the Spark cause chain");
    S3Exception denial = (S3Exception) cause;
    Assertions.assertEquals(403, denial.statusCode());
    Assertions.assertEquals("AccessDenied", denial.awsErrorDetails().errorCode());
  }

  @Override
  protected String getCloudProviderName() {
    return "rustfs";
  }

  @Override
  protected void copyCloudBundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    // The REST server and the catalog use separate classloaders, so each needs its own copy.
    BaseIT.copyBundleJarsToDirectory(
        BUNDLE_NAME, ITUtils.joinPath(gravitinoHome, "iceberg-rest-server", "libs"));
    BaseIT.copyBundleJarsToDirectory(
        BUNDLE_NAME, ITUtils.joinPath(gravitinoHome, "catalogs", "lakehouse-iceberg", "libs"));
  }
}
