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

import java.util.Objects;
import lombok.Getter;

/**
 * The enabled policies of one type matched on a table's nearest level.
 *
 * <p>{@code policyId} is the winner when {@code policyCount} is one.
 */
@Getter
public class NearestPolicyIdPO {

  private Long tableId;
  private Long policyCount;
  private Long policyId;

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NearestPolicyIdPO)) {
      return false;
    }
    NearestPolicyIdPO that = (NearestPolicyIdPO) o;
    return Objects.equals(tableId, that.tableId)
        && Objects.equals(policyCount, that.policyCount)
        && Objects.equals(policyId, that.policyId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, policyCount, policyId);
  }
}
