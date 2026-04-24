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
package org.apache.gravitino.storage.relational.po.auth;

/** Entity change poller result -- one row per entity_change_log entry. */
public class EntityChangeRecord {
  private String metalakeName;
  private String entityType;
  private String fullName;
  private String operateType;
  private long createdAt;

  /** Required by MyBatis for result mapping. */
  public EntityChangeRecord() {}

  public EntityChangeRecord(
      String metalakeName, String entityType, String fullName, String operateType, long createdAt) {
    this.metalakeName = metalakeName;
    this.entityType = entityType;
    this.fullName = fullName;
    this.operateType = operateType;
    this.createdAt = createdAt;
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  public void setMetalakeName(String metalakeName) {
    this.metalakeName = metalakeName;
  }

  public String getEntityType() {
    return entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public String getFullName() {
    return fullName;
  }

  public void setFullName(String fullName) {
    this.fullName = fullName;
  }

  public String getOperateType() {
    return operateType;
  }

  public void setOperateType(String operateType) {
    this.operateType = operateType;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }
}
