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

package org.apache.gravitino.oss.credential;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.OSSTokenCredential;

/**
 * A lightweight credential provider for OSS. It delegates the actual credential generation to
 * {@link OSSTokenCredentialGenerator} which is loaded via reflection to avoid classpath issues.
 */
public class OSSTokenProvider implements CredentialProvider {

  private Map<String, String> properties;
  private volatile CredentialGenerator<OSSTokenCredential> generator;

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public String credentialType() {
    return OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public String getGeneratorClassName() {
    return "org.apache.gravitino.oss.credential.OSSTokenCredentialGenerator";
  }

  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    // Double-checked locking for lazy loading the generator
    if (generator == null) {
      synchronized (this) {
        if (generator == null) {
          this.generator = loadGenerator();
        }
      }
    }

    try {
      return generator.generate(properties, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate OSS token credential", e);
    }
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
