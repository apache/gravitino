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
package org.apache.gravitino.encryption.kms;

import java.util.Optional;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsClient {

  private static final KmsReference REFERENCE =
      new KmsReference(KmsApi.AWS_KMS, "production", "key");

  @Test
  void testReturnsProviderProperties() {
    KmsKeyProperties properties = new TestProperties();
    KmsClient client = reference -> Optional.of(properties);

    Assertions.assertSame(properties, client.getKeyProperties(REFERENCE).orElseThrow());
    Assertions.assertTrue(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void testDefaultClose() {
    KmsClient client = reference -> Optional.of(new TestProperties());

    Assertions.assertDoesNotThrow(client::close);
  }

  @Test
  void testPreservesProviderFailureType() {
    KmsAuthenticationException authenticationFailure =
        new KmsAuthenticationException("authentication failed");
    ConnectionFailedException unavailableFailure =
        new ConnectionFailedException("provider unavailable");
    KmsClient authenticationClient =
        reference -> {
          throw authenticationFailure;
        };
    KmsClient unavailableClient =
        reference -> {
          throw unavailableFailure;
        };

    Assertions.assertSame(
        authenticationFailure,
        Assertions.assertThrows(
            KmsAuthenticationException.class,
            () -> authenticationClient.getKeyProperties(REFERENCE)));
    Assertions.assertSame(
        unavailableFailure,
        Assertions.assertThrows(
            ConnectionFailedException.class, () -> unavailableClient.getKeyProperties(REFERENCE)));
  }

  private static final class TestProperties implements KmsKeyProperties {

    @Override
    public KmsReference reference() {
      return REFERENCE;
    }

    @Override
    public boolean enabled() {
      return true;
    }

    @Override
    public boolean supportsWrapping() {
      return true;
    }

    @Override
    public boolean supportsUnwrapping() {
      return false;
    }
  }
}
