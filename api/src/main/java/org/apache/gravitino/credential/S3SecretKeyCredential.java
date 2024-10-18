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
  private String accessKeyId;
  private String secretAccessKey;

  /**
   * Constructs an instance of {@link S3SecretKeyCredential} with the specified AWS S3 access key ID
   * and secret access key.
   *
   * <p>This constructor validates that both the access key ID and the secret access key are not
   * null. If either of these parameters is null, it will throw a {@link NullPointerException} with
   * an appropriate error message.
   *
   * <p>The provided credentials are used to authenticate requests made to AWS S3 services.
   *
   * @param accessKeyId the AWS S3 access key ID used for authentication. Must not be null.
   * @param secretAccessKey the AWS S3 secret access key used for authentication. Must not be null.
   * @throws NullPointerException if either {@code accessKeyId} or {@code secretAccessKey} is null.
   * @since [Your Library Version]
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
        .put(GRAVITINO_S3_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_S3_SECRET_ACCESS_KEY, secretAccessKey)
        .build();
  }
}
