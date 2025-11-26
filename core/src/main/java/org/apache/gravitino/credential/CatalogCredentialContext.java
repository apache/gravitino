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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

/** CatalogCredentialContext is generated when user requesting catalog credentials. */
public class CatalogCredentialContext implements CredentialContext {
  @NotNull private final String userName;

  public CatalogCredentialContext(String userName) {
    Preconditions.checkArgument(userName != null, "User name should not be null");
    this.userName = userName;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(userName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CatalogCredentialContext)) {
      return false;
    }
    return Objects.equal(userName, ((CatalogCredentialContext) o).userName);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("User name: ").append(userName);
    return stringBuilder.toString();
  }
}
