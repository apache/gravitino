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

import java.util.Objects;
import lombok.Getter;

@Getter
public class CredentialCacheKey {

  private final String credentialType;
  private final CredentialContext credentialContext;

  public CredentialCacheKey(String credentialType, CredentialContext credentialContext) {
    this.credentialType = credentialType;
    this.credentialContext = credentialContext;
  }

  @Override
  public int hashCode() {
    return Objects.hash(credentialType, credentialContext);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CredentialCacheKey)) {
      return false;
    }
    CredentialCacheKey that = (CredentialCacheKey) o;
    return Objects.equals(credentialType, that.credentialType)
        && Objects.equals(credentialContext, that.credentialContext);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("credentialType: ")
        .append(credentialType)
        .append(" credentialContext: ")
        .append(credentialContext);
    return stringBuilder.toString();
  }
}
