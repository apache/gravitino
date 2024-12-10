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

import com.google.common.base.Splitter;
import java.util.Map;
import java.util.stream.Collectors;

public class CatalogCredentialManager {

  public static final String CREDENTIAL_TYPES = "credential-types";
  // private static final String DEFAULT_CREDENTIAL_TYPE = "default-credential-type";

  private static final Splitter splitter = Splitter.on(",");

  // credentialType -> CredentialProvider
  Map<String, CredentialProvider> credentialProviderMap;

  void init(Map<String, String> catalogProperties) {
    String credentialTypes = catalogProperties.get(CREDENTIAL_TYPES);
    // String defaultCredentialType = catalogProperties.get(DEFAULT_CREDENTIAL_TYPE);

    this.credentialProviderMap =
        splitter
            .trimResults()
            .omitEmptyStrings()
            .splitToStream(credentialTypes)
            .collect(
                Collectors.toMap(
                    String::toString,
                    credentialType ->
                        CredentialProviderFactory.create(credentialType, catalogProperties)));
  }

  Credential getCredential(CredentialContext context, String credentialType) {
    CredentialProvider credentialProvider = credentialProviderMap.get(credentialType);
    if (credentialProvider != null) {
      return credentialProvider.getCredential(context);
    } else {
      throw new RuntimeException("No such credential type:" + credentialType);
    }
  }
}
