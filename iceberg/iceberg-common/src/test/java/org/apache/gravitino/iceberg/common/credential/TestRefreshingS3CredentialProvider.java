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
package org.apache.gravitino.iceberg.common.credential;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.credential.CatalogCredentialContext;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.S3TokenCredential;

/** Test credential provider that emits a new S3 session credential on each request. */
public class TestRefreshingS3CredentialProvider implements CredentialProvider {

  /** Test credential provider type. */
  public static final String TYPE = "test-refreshing-s3-token";

  private static final AtomicInteger GENERATED_COUNT = new AtomicInteger();
  private static volatile long expireTimeInMs = System.currentTimeMillis() + 60_000;

  /** Resets this provider's test state. */
  public static void reset() {
    GENERATED_COUNT.set(0);
    expireTimeInMs = System.currentTimeMillis() + 60_000;
  }

  /**
   * Sets the expiration time returned by subsequently generated credentials.
   *
   * @param expireTimeInMs expiration time in milliseconds
   */
  public static void setExpireTimeInMs(long expireTimeInMs) {
    TestRefreshingS3CredentialProvider.expireTimeInMs = expireTimeInMs;
  }

  /**
   * Returns the number of generated credentials.
   *
   * @return generated credential count
   */
  public static int generatedCount() {
    return GENERATED_COUNT.get();
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public String credentialType() {
    return TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof CatalogCredentialContext)) {
      return null;
    }

    int id = GENERATED_COUNT.incrementAndGet();
    return new S3TokenCredential(
        "access-key-" + id, "secret-key-" + id, "session-token-" + id, expireTimeInMs);
  }

  @Override
  public void close() {}
}
