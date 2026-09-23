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

package org.apache.gravitino.cos.credential;

import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.CredentialProviderDelegator;

/**
 * A lightweight credential provider for Tencent Cloud COS. It delegates the actual credential
 * generation to {@link COSTokenGenerator}, which is loaded via reflection so that bundles without
 * the Tencent Cloud STS SDK on the classpath do not fail at class loading time.
 */
public class COSTokenProvider extends CredentialProviderDelegator<COSTokenCredential> {

  @Override
  public boolean supportsScheme(String scheme) {
    // hadoop-cos exposes the `cosn://` scheme, matching `COSFileSystemProvider#scheme()`.
    return "cosn".equalsIgnoreCase(scheme);
  }

  @Override
  public String credentialType() {
    return COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public String getGeneratorClassName() {
    return "org.apache.gravitino.cos.credential.COSTokenGenerator";
  }
}
