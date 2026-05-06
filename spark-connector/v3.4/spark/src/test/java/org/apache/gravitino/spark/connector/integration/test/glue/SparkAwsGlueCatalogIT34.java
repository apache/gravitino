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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration test for Spark 3.4 Glue catalog connector against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b> and only runs when {@code AWS_ACCESS_KEY_ID} is set.
 * Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_DEFAULT_REGION} (optional, defaults to us-east-1)
 *   <li>{@code S3_BUCKET_NAME} (optional, uses default bucket if not set)
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
public class SparkAwsGlueCatalogIT34 extends SparkGlueCatalogIT {

  @BeforeAll
  @Override
  protected void startUp() throws Exception {
    String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String bucketName = System.getenv("S3_BUCKET_NAME");
    System.out.println(
        "[DEBUG] S3_BUCKET_NAME env = " + bucketName + ", default = " + S3_BUCKET_NAME);
    if (bucketName == null) {
      bucketName = S3_BUCKET_NAME;
    }

    // For real AWS, use default Glue endpoint (no custom endpoint)
    // AWS SDK will use the standard endpoint for the given region
    setGlueEndpoint(null);
    setAwsCredentials(accessKeyId, secretAccessKey);

    // Set AWS region from environment variable
    String awsRegion = System.getenv("AWS_DEFAULT_REGION");
    if (awsRegion != null) {
      setAwsRegion(awsRegion);
    }

    // For real AWS, S3 credentials are the same as Glue credentials
    // Use default S3 endpoint (derived from region)
    setS3Credentials(null, accessKeyId, secretAccessKey);

    // Override the default S3 bucket with user-specified bucket.
    // This must be set BEFORE calling super.startUp() because the warehouse
    // path is derived from the bucket name at the top of startUp().
    setS3BucketName(bucketName);

    super.startUp();
  }

  @AfterAll
  @Override
  protected void stop() throws IOException, InterruptedException {
    super.stop();
  }
}
