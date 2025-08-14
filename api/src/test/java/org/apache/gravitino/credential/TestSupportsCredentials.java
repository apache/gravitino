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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSupportsCredentials {
  static class DummyCredential implements Credential {
    private final String type;

    DummyCredential(String type) {
      this.type = type;
    }

    @Override
    public String credentialType() {
      return type;
    }

    @Override
    public long expireTimeInMs() {
      return 0;
    }

    @Override
    public Map<String, String> credentialInfo() {
      return ImmutableMap.of();
    }

    @Override
    public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {}
  }

  static class DummySupports implements SupportsCredentials {
    private final Credential[] credentials;

    DummySupports(Credential... credentials) {
      this.credentials = credentials;
    }

    @Override
    public Credential[] getCredentials() {
      return credentials;
    }
  }

  @Test
  public void testGetCredentialReturnsMatchingCredential() throws Exception {
    Credential c1 = new DummyCredential("typeA");
    Credential c2 = new DummyCredential("typeB");
    SupportsCredentials sc = new DummySupports(c1, c2);
    Assertions.assertEquals(c1, sc.getCredential("typeA"));
  }

  @Test
  public void testGetCredentialThrowsWhenMultipleSameType() {
    Credential c1 = new DummyCredential("typeA");
    Credential c2 = new DummyCredential("typeA");
    SupportsCredentials sc = new DummySupports(c1, c2);
    Assertions.assertThrows(IllegalStateException.class, () -> sc.getCredential("typeA"));
  }
}
