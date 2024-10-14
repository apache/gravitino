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
import java.util.Set;
import lombok.Getter;

public class DummyCredentialProvider implements CredentialProvider {
  Map<String, String> properties;
  static final String CREDENTIAL_TYPE = "dummy";

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    Preconditions.checkArgument(
        context instanceof PathBasedCredentialContext
            || context instanceof CatalogCredentialContext,
        "Doesn't support context: " + context.getClass().getSimpleName());
    if (context instanceof PathBasedCredentialContext) {
      return new DummyCredential((PathBasedCredentialContext) context);
    }
    return null;
  }

  public static class DummyCredential implements Credential {

    @Getter private Set<String> writeLocations;
    @Getter private Set<String> readLocations;

    public DummyCredential(PathBasedCredentialContext locationContext) {
      this.writeLocations = locationContext.getWritePaths();
      this.readLocations = locationContext.getReadPaths();
    }

    @Override
    public String credentialType() {
      return DummyCredentialProvider.CREDENTIAL_TYPE;
    }

    @Override
    public long expireTimeInMs() {
      return 0;
    }

    @Override
    public Map<String, String> credentialInfo() {
      return ImmutableMap.of(
          "writeLocation", writeLocations.toString(), "readLocation", readLocations.toString());
    }
  }
}
