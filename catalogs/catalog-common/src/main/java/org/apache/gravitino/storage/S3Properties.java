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
package org.apache.gravitino.storage;

// Defines the unified S3 properties for different catalogs and connectors.
public class S3Properties {
  // An alternative endpoint of the S3 service, This could be used to for S3FileIO with any
  // s3-compatible object storage service that has a different endpoint, or access a private S3
  // endpoint in a virtual private cloud
  public static final String GRAVITINO_S3_ENDPOINT = "s3-endpoint";
  // The static access key ID used to access S3 data.
  public static final String GRAVITINO_S3_ACCESS_KEY_ID = "s3-access-key-id";
  // The static secret access key used to access S3 data.
  public static final String GRAVITINO_S3_SECRET_ACCESS_KEY = "s3-secret-access-key";
  // The region of the S3 service.
  public static final String GRAVITINO_S3_REGION = "s3-region";
  // S3 role arn
  public static final String GRAVITINO_S3_ROLE_ARN = "s3-role-arn";
  // S3 external id
  public static final String GRAVITINO_S3_EXTERNAL_ID = "s3-external-id";

  // The S3 credentials provider class name.
  public static final String GRAVITINO_S3_CREDS_PROVIDER = "s3-creds-provider";

  private S3Properties() {}
}
