/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** S3 secret key credential. */
public class S3SecretKeyCredential implements Credential {

  /** S3 secret key credential type. */
  public static final String S3_SECRET_KEY_CREDENTIAL_TYPE = "s3-secret-key";
  /** The static access key ID used to access S3 data. */
  public static final String GRAVITINO_S3_STATIC_ACCESS_KEY_ID = "s3-access-key-id";
  /** The static secret access key used to access S3 data. */
  public static final String GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY = "s3-secret-access-key";

  private final String accessKeyId;
  private final String secretAccessKey;

  /**
   * Constructs an instance of {@link S3SecretKeyCredential} with the static S3 access key ID and
   * secret access key.
   *
   * @param accessKeyId The S3 static access key ID.
   * @param secretAccessKey The S3 static secret access key.
   */
  public S3SecretKeyCredential(String accessKeyId, String secretAccessKey) {
    Preconditions.checkNotNull(accessKeyId, "S3 access key Id should not null");
    Preconditions.checkNotNull(secretAccessKey, "S3 secret access key should not null");

    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  @Override
  public String credentialType() {
    return S3_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_S3_STATIC_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY, secretAccessKey)
        .build();
  }

  /**
   * Get S3 static access key ID.
   *
   * @return The S3 access key ID.
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Get S3 static secret access key.
   *
   * @return The S3 secret access key.
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }
}
