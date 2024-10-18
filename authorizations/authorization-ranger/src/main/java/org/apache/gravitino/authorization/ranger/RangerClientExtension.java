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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Ranger client extension <br>
 * The class extends the RangerClient class and provides additional methods to create, search and
 * delete users and groups
 */
public class RangerClientExtension extends RangerClient {
  private static final Logger LOG = LoggerFactory.getLogger(RangerClientExtension.class);
  private static final String URI_USER_BASE = "/service/xusers/users";
  private static final String URI_USER_BY_ID = URI_USER_BASE + "/%d";
  private static final String URI_GROUP_BASE = "/service/xusers/groups";
  private static final String URI_GROUP_BY_ID = URI_GROUP_BASE + "/%d";
  private static final String URI_CREATE_EXTERNAL_USER = URI_USER_BASE + "/external";

  // Ranger user APIs
  private static final API SEARCH_USER = new API(URI_USER_BASE, HttpMethod.GET, Response.Status.OK);
  private static final API CREATE_EXTERNAL_USER =
      new API(URI_CREATE_EXTERNAL_USER, HttpMethod.POST, Response.Status.OK);
  private static final API DELETE_USER =
      new API(URI_USER_BY_ID, HttpMethod.DELETE, Response.Status.NO_CONTENT);

  // Ranger group APIs
  private static final API CREATE_GROUP =
      new API(URI_GROUP_BASE, HttpMethod.POST, Response.Status.OK);
  private static final API SEARCH_GROUP =
      new API(URI_GROUP_BASE, HttpMethod.GET, Response.Status.OK);
  private static final API DELETE_GROUP =
      new API(URI_GROUP_BY_ID, HttpMethod.DELETE, Response.Status.NO_CONTENT);

  // apache/ranger/intg/src/main/java/org/apache/ranger/RangerClient.java
  // The private method callAPI of Ranger is called by reflection
  // private <T> T callAPI(API api, Map<String, String> params, Object request, GenericType<T>
  // responseType) throws RangerServiceException
  private Method callAPIMethodGenericResponseType;

  // private <T> T callAPI(API api, Map<String, String> params, Object request, Class<T>
  // responseType) throws RangerServiceException
  private Method callAPIMethodClassResponseType;
  // private void callAPI(API api, Map<String, String> params) throws RangerServiceException
  private Method callAPIMethodNonResponse;

  public RangerClientExtension(String hostName, String authType, String username, String password) {
    super(hostName, authType, username, password, null);

    // initialize callAPI method
    try {
      callAPIMethodGenericResponseType =
          RangerClient.class.getDeclaredMethod(
              "callAPI", API.class, Map.class, Object.class, GenericType.class);
      callAPIMethodGenericResponseType.setAccessible(true);

      callAPIMethodNonResponse =
          RangerClient.class.getDeclaredMethod("callAPI", API.class, Map.class);
      callAPIMethodNonResponse.setAccessible(true);

      callAPIMethodClassResponseType =
          RangerClient.class.getDeclaredMethod(
              "callAPI", API.class, Map.class, Object.class, Class.class);
      callAPIMethodClassResponseType.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public RangerPolicy createPolicy(RangerPolicy policy) throws RangerServiceException {
    Preconditions.checkArgument(
        policy.getResources().size() > 0, "Ranger policy resources can not be empty!");
    return super.createPolicy(policy);
  }

  public RangerPolicy updatePolicy(long policyId, RangerPolicy policy)
      throws RangerServiceException {
    Preconditions.checkArgument(
        policy.getResources().size() > 0, "Ranger policy resources can not be empty!");
    return super.updatePolicy(policyId, policy);
  }

  public Boolean createUser(VXUser user) throws RuntimeException {
    try {
      callAPIMethodClassResponseType.invoke(this, CREATE_EXTERNAL_USER, null, user, VXUser.class);
    } catch (UniformInterfaceException e) {
      LOG.error("Failed to create user: " + e.getResponse().getEntity(String.class));
      return Boolean.FALSE;
    } catch (InvocationTargetException | IllegalAccessException e) {
      Throwable cause = e.getCause();
      if (cause instanceof com.sun.jersey.api.client.UniformInterfaceException) {
        com.sun.jersey.api.client.UniformInterfaceException uniformException =
            (com.sun.jersey.api.client.UniformInterfaceException) cause;
        int statusCode = uniformException.getResponse().getStatus();
        if (statusCode == 204) {
          // Because Ranger use asynchronously create user, so the response status is 204, research
          // the user to check if it is created
          VXUserList userList = searchUser(ImmutableMap.of("name", user.getName()));
          if (userList.getListSize() == 0) {
            throw new RuntimeException("Failed to create user: " + user.getName(), cause);
          } else {
            return Boolean.TRUE;
          }
        } else {
          throw new RuntimeException(e);
        }
      } else {
        throw new RuntimeException(e);
      }
    }
    return Boolean.TRUE;
  }

  public VXUserList searchUser(Map<String, String> filter) throws RuntimeException {
    try {
      return (VXUserList)
          callAPIMethodClassResponseType.invoke(this, SEARCH_USER, filter, null, VXUserList.class);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean deleteUser(Long userId) throws RuntimeException {
    try {
      Map<String, String> params = ImmutableMap.of("forceDelete", "true");
      callAPIMethodNonResponse.invoke(this, DELETE_USER.applyUrlFormat(userId), params);
    } catch (InvocationTargetException | IllegalAccessException | RangerServiceException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public Boolean createGroup(VXGroup group) throws RuntimeException {
    try {
      callAPIMethodClassResponseType.invoke(this, CREATE_GROUP, null, group, VXGroup.class);
    } catch (UniformInterfaceException e) {
      LOG.error("Failed to create user: " + e.getResponse().getEntity(String.class));
      return Boolean.FALSE;
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return Boolean.TRUE;
  }

  public VXGroupList searchGroup(Map<String, String> filter) throws RuntimeException {
    try {
      return (VXGroupList)
          callAPIMethodClassResponseType.invoke(
              this, SEARCH_GROUP, filter, null, VXGroupList.class);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean deleteGroup(Long groupId) throws RuntimeException {
    try {
      Map<String, String> params = ImmutableMap.of("forceDelete", "true");
      callAPIMethodNonResponse.invoke(this, DELETE_GROUP.applyUrlFormat(groupId), params);
    } catch (InvocationTargetException | IllegalAccessException | RangerServiceException e) {
      throw new RuntimeException(e);
    }
    return true;
  }
}
