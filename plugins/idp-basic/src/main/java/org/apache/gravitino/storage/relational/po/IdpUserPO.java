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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(builderClassName = "Builder", setterPrefix = "with")
public class IdpUserPO {
  private Long userId;
  private String userName;
  private String passwordHash;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public static class Builder {
    private void validate() {
      Preconditions.checkArgument(userId != null, "User id is required");
      Preconditions.checkArgument(userName != null, "User name is required");
      Preconditions.checkArgument(passwordHash != null, "Password hash is required");
      Preconditions.checkArgument(currentVersion != null, "Current version is required");
      Preconditions.checkArgument(lastVersion != null, "Last version is required");
      Preconditions.checkArgument(deletedAt != null, "Deleted at is required");
    }

    public IdpUserPO build() {
      validate();
      return new IdpUserPO(userId, userName, passwordHash, currentVersion, lastVersion, deletedAt);
    }
  }
}
