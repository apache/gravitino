/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

/**
 * Where the Hive a {@link HiveFixture} runs keeps table data besides its own HDFS. Every mode but
 * {@link #HDFS} gets its own Hive container.
 */
public enum HiveStorage {
  /** HDFS inside the Hive container only. */
  HDFS,
  /** S3 on the run's LocalStack, which starts first. */
  S3,
  /**
   * Google Cloud Storage, with the service account file named by the {@code
   * GCS_SERVICE_ACCOUNT_JSON_PATH} environment variable.
   */
  GCS,
  /**
   * Azure Blob Storage, with the account in the {@code ABS_ACCOUNT_NAME} and {@code
   * ABS_ACCOUNT_KEY} environment variables.
   */
  ABS
}
