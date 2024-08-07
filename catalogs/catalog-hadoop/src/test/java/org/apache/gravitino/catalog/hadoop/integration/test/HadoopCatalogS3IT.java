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
package org.apache.gravitino.catalog.hadoop.integration.test;

import static org.apache.gravitino.integration.test.container.S3MockContainer.HTTP_PORT;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.authentication.aws.AwsConfig;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HadoopCatalogS3IT extends HadoopCatalogCommonIT {
  private static final String DEFAULT_BASE_LOCATION = "s3a://gravitino-fileset-IT/";
  private static final String DEFAULT_AK = "foo";
  private static final String DEFAULT_SK = "bar";

  private static String s3Endpoint;

  @Override
  protected void startContainer() {
    containerSuite.startS3MockContainer(ImmutableMap.of("initialBuckets", "gravitino-fileset-IT"));
    s3Endpoint =
        String.format(
            "http://%s:%s/",
            containerSuite.getS3MockContainer().getContainerIpAddress(), HTTP_PORT);
  }

  @Override
  protected Configuration hadoopConf() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_BASE_LOCATION);
    conf.set("fs.s3a.access.key", DEFAULT_AK);
    conf.set("fs.s3a.secret.key", DEFAULT_SK);
    conf.set("fs.s3a.endpoint", s3Endpoint);
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    return conf;
  }

  @Override
  protected String defaultBaseLocation() {
    return DEFAULT_BASE_LOCATION;
  }

  @Override
  protected Map<String, String> catalogProperties() {
    return ImmutableMap.<String, String>builder()
        .put(AuthenticationConfig.AUTH_TYPE_KEY, "aws")
        .put(AwsConfig.ENDPOINT, s3Endpoint)
        .put(AwsConfig.ACCESS_KEY, DEFAULT_AK)
        .put(AwsConfig.SECRET_KEY, DEFAULT_SK)
        .build();
  }

  @Override
  protected Map<String, String> schemaProperties() {
    return ImmutableMap.<String, String>builder()
        .put("key1", "val1")
        .put("key2", "val2")
        .put("location", DEFAULT_BASE_LOCATION)
        .build();
  }
}
