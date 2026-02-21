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

package org.apache.gravitino.s3.credential;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.gravitino.credential.CredentialProviderDelegator;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.config.S3CredentialConfig;

/**
 * A lightweight credential provider for S3. It delegates the actual credential generation to {@link
 * S3TokenGenerator} which is loaded via reflection to avoid classpath issues.
 */
public class S3TokenProvider extends CredentialProviderDelegator<S3TokenCredential> {

  @Override
  public String credentialType() {
    return S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public String getGeneratorClassName() {
    return "org.apache.gravitino.s3.credential.S3TokenGenerator";
  }

  @Override
  public Set<String> sensitivePropertyKeys() {
    return Sets.newHashSet(
        S3CredentialConfig.S3_ACCESS_KEY_ID.getKey(),
        S3CredentialConfig.S3_SECRET_ACCESS_KEY.getKey());
  }
}
