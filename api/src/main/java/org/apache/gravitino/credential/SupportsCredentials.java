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

import java.util.Optional;
import org.apache.gravitino.exceptions.CredentialDeserializeException;
import org.apache.gravitino.exceptions.NoSuchCredentialException;

/** Interface to get credentials. */
public interface SupportsCredentials {
  /**
   * Retrieves an array of {@link Credential} objects based on the specified credential type.
   *
   * @param credentialType The server will return credentials according to the configuration if
   *     credential type is empty, or else will return the credential with the specified credential
   *     type.
   * @return An array of {@link Credential} objects. There will be only one credential for one
   *     credential type. In most cases the array only contains one credential. If the object like
   *     {@link org.apache.gravitino.file.Fileset} contains multi locations for different storages
   *     like HDFS, S3, etc. The array will contain multi credentials. The array could be empty if
   *     you request a credential for a catalog but the credential provider couldn't generate the
   *     credential for the catalog, like S3 token credential provider only generate credential for
   *     the specific object like {@link org.apache.gravitino.file.Fileset}, {@link
   *     org.apache.gravitino.rel.Table}.
   * @throws NoSuchCredentialException If the specified credential type cannot be found in the
   *     server side.
   * @throws CredentialDeserializeException If there are problems when deserializing the credential
   *     in client side.
   */
  Credential[] getCredentials(Optional<String> credentialType)
      throws NoSuchCredentialException, CredentialDeserializeException;
}
