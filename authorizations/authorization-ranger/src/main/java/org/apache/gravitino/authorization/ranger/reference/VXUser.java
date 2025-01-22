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

import java.util.Collection;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VXUser.java
@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VXUser extends VXDataObject implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  /** Name. */
  protected String name;
  /** First Name. */
  protected String firstName;
  /** Last Name. */
  protected String lastName;
  /** Email address. */
  protected String emailAddress;
  /** Password. */
  protected String password = "rangerR0cks!";
  /** Description */
  protected String description;
  /** Id of the credential store */
  protected Long credStoreId;
  /** List of group ids for this user. */
  protected Collection<Long> groupIdList;

  protected Collection<String> groupNameList;
  protected int status = RangerCommonEnums.STATUS_ENABLED;
  protected Integer isVisible = RangerCommonEnums.IS_VISIBLE;
  protected int userSource;
  /** List of roles for this user */
  protected Collection<String> userRoleList;
  /** Additional store attributes. */
  protected String otherAttributes;
  /** Sync Source */
  protected String syncSource;

  /** Default constructor. This will set all the attributes to default value. */
  public VXUser() {
    isVisible = RangerCommonEnums.IS_VISIBLE;
  }

  public String getName() {
    return name;
  }

  /**
   * This return the bean content in string format
   *
   * @return formatedStr
   */
  @Override
  public String toString() {
    String str = "VXUser={";
    str += super.toString();
    str += "name={" + name + "} ";
    str += "firstName={" + firstName + "} ";
    str += "lastName={" + lastName + "} ";
    str += "emailAddress={" + emailAddress + "} ";
    str += "description={" + description + "} ";
    str += "credStoreId={" + credStoreId + "} ";
    str += "isVisible={" + isVisible + "} ";
    str += "groupIdList={" + groupIdList + "} ";
    str += "groupNameList={" + groupNameList + "} ";
    str += "roleList={" + userRoleList + "} ";
    str += "otherAttributes={" + otherAttributes + "} ";
    str += "syncSource={" + syncSource + "} ";
    str += "}";
    return str;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final VXUser vxUser;

    public Builder() {
      this.vxUser = new VXUser();
    }

    public Builder withName(String name) {
      vxUser.name = name;
      return this;
    }

    public Builder withDescription(String description) {
      vxUser.description = description;
      return this;
    }

    public VXUser build() {
      return vxUser;
    }
  }
}
