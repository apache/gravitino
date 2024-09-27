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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;

public class TestCredential implements Credential {

  private Set<String> writeLocations;
  private Set<String> readLocations;

  public TestCredential(LocationContext locationContext) {
    this.writeLocations = locationContext.getWriteLocations();
    this.readLocations = locationContext.getReadLocations();
  }

  @Override
  public String getCredentialType() {
    return TestCredentialProvider.CREDENTIAL_TYPE;
  }

  @Override
  public long getExpireTime() {
    return 0;
  }

  @Override
  public Map<String, String> getCredentialInfo() {
    return ImmutableMap.of(
        "token-test",
        "test",
        "writeLocation",
        writeLocations.toString(),
        "readLocation",
        readLocations.toString());
  }
}
