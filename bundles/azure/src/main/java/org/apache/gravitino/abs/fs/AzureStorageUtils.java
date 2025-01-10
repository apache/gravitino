/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.abs.fs;

import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;

public class AzureStorageUtils {

  /**
   * Get the ADLS credential from the credential array. Use the account and secret if dynamic token
   * is not found, null if both are not found.
   *
   * @param credentials The credential array.
   * @return A credential. Null if not found.
   */
  static Credential getSuitableCredential(Credential[] credentials) {
    for (Credential credential : credentials) {
      if (credential instanceof ADLSTokenCredential) {
        return credential;
      }
    }

    for (Credential credential : credentials) {
      if (credential instanceof AzureAccountKeyCredential) {
        return credential;
      }
    }

    return null;
  }

  /**
   * Get the ADLS token credential from the credential array. Null if not found.
   *
   * @param credentials The credential array.
   * @return A credential. Null if not found.
   */
  static Credential getADLSTokenCredential(Credential[] credentials) {
    for (Credential credential : credentials) {
      if (credential instanceof ADLSTokenCredential) {
        return credential;
      }
    }

    return null;
  }
}
