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
 * One ancestry level for a table used as input to nearest-policy match SQL.
 *
 * <p>{@code lvl} is 0 for the table itself, then increases toward the catalog (nearest parent
 * first), matching {@code EffectiveTagResolver} walk order.
 */
public class NearestPolicyChainPO {

  private Long tableId;
  private Long objectId;
  private String objectType;
  private Integer lvl;

  /**
   * @return table whose nearest policy is being resolved
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
   * @return metadata object id at this chain level (table, schema, or catalog id)
   */
  public Long getObjectId() {
    return objectId;
  }

  /**
   * @param objectId metadata object id
   */
  public void setObjectId(Long objectId) {
    this.objectId = objectId;
  }

  /**
   * @return {@link org.apache.gravitino.MetadataObject.Type} name
   */
  public String getObjectType() {
    return objectType;
  }

  /**
   * @param objectType metadata object type name
   */
  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  /**
   * @return ancestry depth (0 = table)
   */
  public Integer getLvl() {
    return lvl;
  }

  /**
   * @param lvl ancestry depth
   */
  public void setLvl(Integer lvl) {
    this.lvl = lvl;
  }

  /**
   * Creates a chain row.
   *
   * @param tableId table id
   * @param objectId object id at this level
   * @param objectType object type name
   * @param lvl ancestry depth
   * @return chain row
   */
  public static NearestPolicyChainPO of(long tableId, long objectId, String objectType, int lvl) {
    NearestPolicyChainPO po = new NearestPolicyChainPO();
    po.setTableId(tableId);
    po.setObjectId(objectId);
    po.setObjectType(objectType);
    po.setLvl(lvl);
    return po;
  }
}
