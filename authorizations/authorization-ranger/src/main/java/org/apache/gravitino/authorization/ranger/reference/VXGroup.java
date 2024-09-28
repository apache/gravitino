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

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VXGroup.java
@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VXGroup extends VXDataObject implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  /** Name */
  protected String name;
  /** Description */
  protected String description;
  /** Type of group This attribute is of type enum CommonEnums::XAGroupType */
  protected int groupType = 0;

  protected int groupSource = RangerCommonEnums.GROUP_INTERNAL;
  /** Id of the credential store */
  protected Long credStoreId;
  /** Group visibility */
  protected Integer isVisible;
  /** Additional store attributes. */
  protected String otherAttributes;
  /** Sync Source Attribute */
  protected String syncSource;

  /** Default constructor. This will set all the attributes to default value. */
  public VXGroup() {
    groupType = 0;
    isVisible = RangerCommonEnums.IS_VISIBLE;
  }

  /**
   * This return the bean content in string format
   *
   * @return formatedStr
   */
  public String toString() {
    String str = "VXGroup={";
    str += super.toString();
    str += "name={" + name + "} ";
    str += "description={" + description + "} ";
    str += "groupType={" + groupType + "} ";
    str += "credStoreId={" + credStoreId + "} ";
    str += "isVisible={" + isVisible + "} ";
    str += "groupSrc={" + groupSource + "} ";
    str += "otherAttributes={" + otherAttributes + "} ";
    str += "syncSource={" + syncSource + "} ";
    str += "}";
    return str;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final VXGroup vxGroup;

    public Builder() {
      this.vxGroup = new VXGroup();
    }

    public Builder withName(String name) {
      vxGroup.name = name;
      return this;
    }

    public Builder withDescription(String description) {
      vxGroup.description = description;
      return this;
    }

    public VXGroup build() {
      return vxGroup;
    }
  }
}
