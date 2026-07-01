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
package org.apache.gravitino.iceberg.service.extension;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.S3SecretKeyCredential;

/** Test credential provider that returns static S3 keys for remote-signing unit tests. */
public class S3SigningTestCredentialProvider implements CredentialProvider {

  /** Credential type identifier for test configuration. */
  public static final String CREDENTIAL_TYPE = "iceberg-rest-s3-signing-test";

  private static final S3SecretKeyCredential TEST_CREDENTIAL =
      new S3SecretKeyCredential("test-access-key", "test-secret-key");

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public String credentialType() {
    return CREDENTIAL_TYPE;
  }

  @Override
  public boolean supportsScheme(String scheme) {
    return "s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme);
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    return TEST_CREDENTIAL;
  }

  @Override
  public void close() throws IOException {}
}
