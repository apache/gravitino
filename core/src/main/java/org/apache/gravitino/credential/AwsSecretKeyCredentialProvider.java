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
package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Generates static AWS access-key credentials for Glue API authentication. */
public class AwsSecretKeyCredentialProvider implements CredentialProvider {

  private String accessKeyId;
  private String secretAccessKey;

  @Override
  public void initialize(Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    this.accessKeyId = properties.get(AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID);
    this.secretAccessKey = properties.get(AwsSecretKeyCredential.GRAVITINO_AWS_SECRET_ACCESS_KEY);
    boolean hasAccessKeyId = StringUtils.isNotBlank(accessKeyId);
    boolean hasSecretAccessKey = StringUtils.isNotBlank(secretAccessKey);
    Preconditions.checkArgument(
        hasAccessKeyId == hasSecretAccessKey,
        "Incomplete AWS static credential pair for %s: both %s and %s are required",
        AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE,
        AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID,
        AwsSecretKeyCredential.GRAVITINO_AWS_SECRET_ACCESS_KEY);
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public boolean supportsScheme(String scheme) {
    return false;
  }

  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(secretAccessKey)) {
      return null;
    }
    return new AwsSecretKeyCredential(accessKeyId, secretAccessKey);
  }
}
