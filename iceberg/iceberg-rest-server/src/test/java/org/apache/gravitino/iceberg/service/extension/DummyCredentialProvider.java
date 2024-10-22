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

package org.apache.gravitino.iceberg.service.extension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;

public class DummyCredentialProvider implements CredentialProvider {
  public static final String DUMMY_CREDENTIAL_TYPE = "iceberg-rest-dummy-test";

  public static class SimpleCredential implements Credential {
    @Override
    public String credentialType() {
      return DUMMY_CREDENTIAL_TYPE;
    }

    @Override
    public long expireTimeInMs() {
      return 0;
    }

    @Override
    public Map<String, String> credentialInfo() {
      return new HashMap<>();
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public String credentialType() {
    return DUMMY_CREDENTIAL_TYPE;
  }

  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    return new SimpleCredential();
  }

  @Override
  public void close() throws IOException {}
}
