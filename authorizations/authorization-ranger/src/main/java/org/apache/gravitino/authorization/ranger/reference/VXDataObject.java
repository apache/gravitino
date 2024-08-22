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
package org.apache.gravitino.authorization.ranger.reference;

import java.util.Date;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VXDataObject.java
@JsonAutoDetect(
    getterVisibility = Visibility.NONE,
    setterVisibility = Visibility.NONE,
    fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VXDataObject implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  /** Id of the data */
  protected Long id;
  /** Date when this data was created */
  @JsonSerialize(using = JsonDateSerializer.class)
  protected Date createDate;
  /** Date when this data was updated */
  @JsonSerialize(using = JsonDateSerializer.class)
  protected Date updateDate;
  /** Owner */
  protected String owner;
  /** Updated By */
  protected String updatedBy;
  /** Default constructor. This will set all the attributes to default value. */
  public VXDataObject() {}

  /**
   * This method sets the value to the member attribute <b>id</b>. You cannot set null to the
   * attribute.
   *
   * @param id Value to set member attribute <b>id</b>
   */
  public void setId(Long id) {
    this.id = id;
  }

  /**
   * Returns the value for the member attribute <b>id</b>
   *
   * @return Long - value of member attribute <b>id</b>.
   */
  public Long getId() {
    return this.id;
  }

  /**
   * This return the bean content in string format
   *
   * @return formatedStr
   */
  public String toString() {
    String str = "VXDataObject={";
    str += super.toString();
    str += "id={" + id + "} ";
    str += "createDate={" + createDate + "} ";
    str += "updateDate={" + updateDate + "} ";
    str += "owner={" + owner + "} ";
    str += "updatedBy={" + updatedBy + "} ";
    str += "}";
    return str;
  }
}
