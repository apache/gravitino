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

import java.util.Map;
import org.apache.gravitino.credential.DummyCredentialProvider.DummyCredential;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class TestCredentialProvider {
  @Test
  void testCredentialProvider() {
    Map<String, String> catalogProperties = ImmutableMap.of("a", "b");
    CredentialProvider credentialProvider =
        CredentialProviderFactory.create(
            DummyCredentialProvider.CREDENTIAL_TYPE, catalogProperties);
    Assertions.assertEquals(
        DummyCredentialProvider.CREDENTIAL_TYPE, credentialProvider.credentialType());
    Assertions.assertTrue(credentialProvider instanceof DummyCredentialProvider);
    DummyCredentialProvider dummyCredentialProvider = (DummyCredentialProvider) credentialProvider;
    Assertions.assertEquals(catalogProperties, dummyCredentialProvider.properties);

    ImmutableSet<String> writeLocations = ImmutableSet.of("location1");
    ImmutableSet<String> readLocations = ImmutableSet.of("location2");

    PathBasedCredentialContext locationContext =
        new PathBasedCredentialContext("user", writeLocations, readLocations);
    Credential credential = dummyCredentialProvider.getCredential(locationContext);
    Assertions.assertTrue(credential instanceof DummyCredential);
    DummyCredential dummyCredential = (DummyCredential) credential;

    Assertions.assertEquals(writeLocations, dummyCredential.getWriteLocations());
    Assertions.assertEquals(readLocations, dummyCredential.getReadLocations());
  }
}
