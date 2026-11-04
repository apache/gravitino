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

/**
 * The enabled policies of one type matched on a table's nearest level.
 *
 * <p>{@code minPolicyId} equals {@code maxPolicyId} when exactly one policy wins that level.
 */
public class NearestPolicyIdPO {

  private Long tableId;
  private Long minPolicyId;
  private Long maxPolicyId;

  /**
   * @return table id
   */
  public Long getTableId() {
    return tableId;
  }

  /**
   * @param tableId table id
   */
  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  /**
   * @return smallest policy id matched on the nearest level
   */
  public Long getMinPolicyId() {
    return minPolicyId;
  }

  /**
   * @param minPolicyId smallest policy id matched on the nearest level
   */
  public void setMinPolicyId(Long minPolicyId) {
    this.minPolicyId = minPolicyId;
  }

  /**
   * @return largest policy id matched on the nearest level
   */
  public Long getMaxPolicyId() {
    return maxPolicyId;
  }

  /**
   * @param maxPolicyId largest policy id matched on the nearest level
   */
  public void setMaxPolicyId(Long maxPolicyId) {
    this.maxPolicyId = maxPolicyId;
  }
}
