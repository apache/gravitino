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
package org.apache.gravitino.lance.service.extension;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.S3TokenCredential;

/**
 * A test credential provider that returns a fixed S3TokenCredential. Used in IT tests to verify
 * credential vending without requiring real AWS infrastructure.
 */
public class LanceDummyCredentialProvider implements CredentialProvider {

  public static final String CREDENTIAL_TYPE = "lance-dummy-test";
  private static final String DUMMY_AK = "test-access-key-id";
  private static final String DUMMY_SK = "test-secret-access-key";
  private static final String DUMMY_TOKEN = "test-session-token";
  private static final long DUMMY_EXPIRE_MS = 3600000L;

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public String credentialType() {
    return CREDENTIAL_TYPE;
  }

  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    return new S3TokenCredential(DUMMY_AK, DUMMY_SK, DUMMY_TOKEN, DUMMY_EXPIRE_MS);
  }

  @Override
  public void close() throws IOException {}
}
