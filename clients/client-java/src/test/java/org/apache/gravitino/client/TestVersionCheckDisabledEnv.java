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

package org.apache.gravitino.client;

import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestVersionCheckDisabledEnv {

  private static class TestClient extends GravitinoClientBase {
    protected TestClient(String uri, AuthDataProvider authDataProvider, boolean checkVersion) {
      super(uri, authDataProvider, checkVersion, Collections.emptyMap(), Collections.emptyMap());
    }
  }

  private static class TestBuilder extends GravitinoClientBase.Builder<TestClient> {
    private static String envValue;

    protected TestBuilder() {
      super("http://localhost:12345");
    }

    @Override
    protected String versionCheckDisabledEnvValue() {
      return envValue;
    }

    @Override
    public TestClient build() {
      return new TestClient(uri, authDataProvider, isVersionCheckEnabled());
    }

    private boolean isEnabled() {
      return isVersionCheckEnabled();
    }
  }

  @AfterEach
  public void resetEnvValue() {
    TestBuilder.envValue = null;
  }

  @Test
  public void testEnvDisablesVersionCheck() {
    TestBuilder.envValue = "true";
    TestBuilder builder = new TestBuilder();

    Assertions.assertFalse(builder.isEnabled());
  }

  @Test
  public void testEnvIsCaseInsensitive() {
    TestBuilder.envValue = "TrUe";
    TestBuilder builder = new TestBuilder();

    Assertions.assertFalse(builder.isEnabled());
  }

  @Test
  public void testEnvFalseKeepsVersionCheckEnabled() {
    TestBuilder.envValue = "false";
    TestBuilder builder = new TestBuilder();

    Assertions.assertTrue(builder.isEnabled());
  }

  @Test
  public void testExplicitDisableOverridesEnvFalse() {
    TestBuilder.envValue = "false";
    TestBuilder builder = new TestBuilder();
    builder.withVersionCheckDisabled();

    Assertions.assertFalse(builder.isEnabled());
  }
}
