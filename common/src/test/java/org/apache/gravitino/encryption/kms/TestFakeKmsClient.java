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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFakeKmsClient extends TestKmsClientContract {

  private static final String PROVIDER = "test";
  private static final String USABLE_KEY = "usable";
  private static final String DISABLED_KEY = "disabled";
  private static final String MISSING_KEY = "missing";

  private final FakeKmsClient client =
      new FakeKmsClient(PROVIDER)
          .putKey(USABLE_KEY, true, true, true)
          .putKey(DISABLED_KEY, false, true, true);

  @Test
  void testRejectsBlankProvider() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new FakeKmsClient(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new FakeKmsClient(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new FakeKmsClient(" "));
  }

  @Test
  void testNormalizesProvider() {
    FakeKmsClient paddedProviderClient = new FakeKmsClient(" " + PROVIDER + " ");

    Assertions.assertDoesNotThrow(
        () -> paddedProviderClient.getKeyProperties(new KmsReference(PROVIDER, MISSING_KEY)));
  }

  @Test
  void testReportsDisabledKeyAsPresent() {
    KmsKeyProperties properties =
        client.getKeyProperties(new KmsReference(PROVIDER, DISABLED_KEY)).orElseThrow();

    Assertions.assertFalse(properties.enabled());
  }

  @Override
  protected KmsClient client() {
    return client;
  }

  @Override
  protected KmsReference usableKey() {
    return new KmsReference(PROVIDER, USABLE_KEY);
  }

  @Override
  protected KmsReference missingKey() {
    return new KmsReference(PROVIDER, MISSING_KEY);
  }
}
