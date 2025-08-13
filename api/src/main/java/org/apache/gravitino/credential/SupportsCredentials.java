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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchCredentialException;

/** Interface to get credentials. */
public interface SupportsCredentials {

  /**
   * Retrieves an array of {@link Credential} objects.
   *
   * @return An array of {@link Credential} objects. In most cases the array only contains one
   *     credential. If the object like {@link org.apache.gravitino.file.Fileset} contains multiple
   *     locations for different storages like HDFS, S3, the array will contain multiple
   *     credentials. The array could be empty if you request a credential for a catalog but the
   *     credential provider couldn't generate the credential for the catalog, like S3 token
   *     credential provider only generate credential for the specific object like {@link
   *     org.apache.gravitino.file.Fileset}, {@link org.apache.gravitino.rel.Table}. There will be
   *     at most one credential for one credential type.
   */
  Credential[] getCredentials();

  /**
   * Retrieves an {@link Credential} object based on the specified credential type.
   *
   * @param credentialType The type of the credential like s3-token, s3-secret-key which defined in
   *     the specific credentials.
   * @return An {@link Credential} object with the specified credential type.
   * @throws NoSuchCredentialException If the specific credential cannot be found.
   */
  default Credential getCredential(String credentialType) throws NoSuchCredentialException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(credentialType), "Credential type should not be empty");
    Credential[] credentials = getCredentials();
    if (credentials.length == 0) {
      throw new NoSuchCredentialException(
          "No credential found for the credential type: %s", credentialType);
    }

    List<Credential> filteredCredentials =
        Arrays.stream(credentials)
            .filter(credential -> credentialType.equals(credential.credentialType()))
            .collect(Collectors.toList());
    if (filteredCredentials.isEmpty()) {
      throw new NoSuchCredentialException(
          "No credential found for the credential type: %s", credentialType);
    } else if (filteredCredentials.size() > 1) {
      throw new IllegalStateException(
          "Multiple credentials found for the credential type:" + credentialType);
    }

    return filteredCredentials.get(0);
  }
}
