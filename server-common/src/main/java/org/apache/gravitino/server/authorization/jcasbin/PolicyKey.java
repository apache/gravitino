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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.Objects;

/**
 * Composite key of the per-role policy index: {@code (metadataType, metadataId, privilege)}. It
 * mirrors the non-subject fields of a jcasbin {@code p} policy row so that an index lookup answers
 * the same question the old {@code enforcer.enforce} matcher did for one role, but in {@code O(1)}
 * instead of a linear scan over every policy line.
 *
 * <p>The three fields are compared with exact equality, matching the jcasbin matcher's string
 * comparison; {@code metadataType} and {@code privilege} are stored as they are passed on the
 * authorization hot path (already upper-cased enum names).
 */
final class PolicyKey {

  private final String metadataType;
  private final long metadataId;
  private final String privilege;
  private final int hash;

  PolicyKey(String metadataType, long metadataId, String privilege) {
    this.metadataType = metadataType;
    this.metadataId = metadataId;
    this.privilege = privilege;
    this.hash = Objects.hash(metadataType, metadataId, privilege);
  }

  String privilege() {
    return privilege;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyKey)) {
      return false;
    }
    PolicyKey other = (PolicyKey) o;
    return hash == other.hash
        && metadataId == other.metadataId
        && Objects.equals(metadataType, other.metadataType)
        && Objects.equals(privilege, other.privilege);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return "PolicyKey{type=" + metadataType + ", id=" + metadataId + ", priv=" + privilege + '}';
  }
}
