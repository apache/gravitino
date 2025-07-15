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

package org.apache.gravitino.listener.api.info;

import com.google.common.base.Objects;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Owner;

/** Owner information that used in owner operation events. */
@DeveloperApi
public class OwnerInfo {
  private final String name;
  private final Owner.Type type;

  public OwnerInfo(String name, Owner.Type type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Owner.Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OwnerInfo)) {
      return false;
    }
    OwnerInfo ownerInfo = (OwnerInfo) o;
    return Objects.equal(name, ownerInfo.name) && type == ownerInfo.type;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, type);
  }
}
