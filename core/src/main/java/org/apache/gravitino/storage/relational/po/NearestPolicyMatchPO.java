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
 * One candidate policy-tag assignment row for nearest-policy resolution.
 *
 * <p>Rows are produced by SQL over the ancestry chain; Java applies effective-tag override,
 * selector matching, and nearest-level uniqueness (same semantics as {@code ObjectPolicyResolver} +
 * maintenance nearest pick).
 */
public class NearestPolicyMatchPO {

  private Long tableId;
  private Integer lvl;
  private String tagName;
  private String tagValue;
  private Long policyId;
  private String selector;

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
   * @return ancestry depth of the tag assignment source
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
   * @return tag name
   */
  public String getTagName() {
    return tagName;
  }

  /**
   * @param tagName tag name
   */
  public void setTagName(String tagName) {
    this.tagName = tagName;
  }

  /**
   * @return raw tag assignment value (empty string means no value)
   */
  public String getTagValue() {
    return tagValue;
  }

  /**
   * @param tagValue tag assignment value
   */
  public void setTagValue(String tagValue) {
    this.tagValue = tagValue;
  }

  /**
   * @return enabled policy id of the requested type
   */
  public Long getPolicyId() {
    return policyId;
  }

  /**
   * @param policyId policy id
   */
  public void setPolicyId(Long policyId) {
    this.policyId = policyId;
  }

  /**
   * @return policy-tag selector JSON, or null for all-values
   */
  public String getSelector() {
    return selector;
  }

  /**
   * @param selector selector JSON
   */
  public void setSelector(String selector) {
    this.selector = selector;
  }
}
