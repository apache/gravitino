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
package org.apache.gravitino.catalog.hadoop.fs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class TestInMemoryFileSystemCredentialsProvider {

  @Test
  void testCredentialRegistrationAndCleanup() {
    Credential[] credentials = new Credential[0];
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    Configuration conf = new Configuration(false);
    conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);

    InMemoryFileSystemCredentialsProvider provider = new InMemoryFileSystemCredentialsProvider();
    provider.setConf(conf);
    assertArrayEquals(credentials, provider.getCredentials());

    InMemoryFileSystemCredentialsProvider.unregister(handle);
    assertThrows(IllegalStateException.class, provider::getCredentials);
  }
}
