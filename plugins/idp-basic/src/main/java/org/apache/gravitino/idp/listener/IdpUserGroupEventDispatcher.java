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
package org.apache.gravitino.idp.listener;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.listener.event.IdpAddGroupEvent;
import org.apache.gravitino.idp.listener.event.IdpAddGroupFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpAddGroupPreEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserPreEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeGroupMembershipEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeGroupMembershipFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeGroupMembershipPreEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeUserPasswordEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeUserPasswordFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeUserPasswordPreEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveGroupEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveGroupFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveGroupPreEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveUserEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveUserFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveUserPreEvent;
import org.apache.gravitino.idp.listener.event.IdpUpdateUserEnabledEvent;
import org.apache.gravitino.idp.listener.event.IdpUpdateUserEnabledFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpUpdateUserEnabledPreEvent;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Dispatches pre/success/failure events around built-in IdP user and group mutations, matching the
 * {@code *EventDispatcher} pattern used for metalake access-control and job operations.
 *
 * <p>Success and failure events mark the request thread so {@code HttpAuditFilter} does not emit a
 * redundant {@code HttpRequestEvent} / {@code HttpRequestFailureEvent} with {@code UNKNOWN} and a
 * null object identifier.
 */
public class IdpUserGroupEventDispatcher {

  private final EventBus eventBus;
  private final IdpUserGroupManager manager;

  public IdpUserGroupEventDispatcher(EventBus eventBus, IdpUserGroupManager manager) {
    this.eventBus = eventBus;
    this.manager = manager;
  }

  public IdpUser getUser(String username) {
    return manager.getUser(username);
  }

  public IdpGroup getGroup(String groupName) {
    return manager.getGroup(groupName);
  }

  public IdpUser addUser(String username, String password, boolean enabled) throws IOException {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpAddUserPreEvent(initiator, username));
    try {
      IdpUser user = manager.addUser(username, password, enabled);
      eventBus.dispatchEvent(new IdpAddUserEvent(initiator, username));
      return user;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpAddUserFailureEvent(initiator, username, e));
      throw e;
    }
  }

  public boolean removeUser(String username) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpRemoveUserPreEvent(initiator, username));
    try {
      boolean isExists = manager.removeUser(username);
      eventBus.dispatchEvent(new IdpRemoveUserEvent(initiator, username, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpRemoveUserFailureEvent(initiator, username, e));
      throw e;
    }
  }

  public boolean changePassword(String username, String password) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpChangeUserPasswordPreEvent(initiator, username));
    try {
      boolean updated = manager.changePassword(username, password);
      eventBus.dispatchEvent(new IdpChangeUserPasswordEvent(initiator, username));
      return updated;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpChangeUserPasswordFailureEvent(initiator, username, e));
      throw e;
    }
  }

  public boolean changePassword(String username, String newPassword, String oldPassword) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpChangeUserPasswordPreEvent(initiator, username));
    try {
      boolean updated = manager.changePassword(username, newPassword, oldPassword);
      eventBus.dispatchEvent(new IdpChangeUserPasswordEvent(initiator, username));
      return updated;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpChangeUserPasswordFailureEvent(initiator, username, e));
      throw e;
    }
  }

  public boolean updateEnabled(String username, boolean enabled) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpUpdateUserEnabledPreEvent(initiator, username, enabled));
    try {
      boolean updated = manager.updateEnabled(username, enabled);
      eventBus.dispatchEvent(new IdpUpdateUserEnabledEvent(initiator, username, enabled));
      return updated;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpUpdateUserEnabledFailureEvent(initiator, username, enabled, e));
      throw e;
    }
  }

  public IdpGroup addGroup(String groupName, String comment) throws IOException {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpAddGroupPreEvent(initiator, groupName));
    try {
      IdpGroup group = manager.addGroup(groupName, comment);
      eventBus.dispatchEvent(new IdpAddGroupEvent(initiator, groupName));
      return group;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpAddGroupFailureEvent(initiator, groupName, e));
      throw e;
    }
  }

  public boolean removeGroup(String groupName, boolean force) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new IdpRemoveGroupPreEvent(initiator, groupName, force));
    try {
      boolean isExists = manager.removeGroup(groupName, force);
      eventBus.dispatchEvent(new IdpRemoveGroupEvent(initiator, groupName, isExists, force));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new IdpRemoveGroupFailureEvent(initiator, groupName, force, e));
      throw e;
    }
  }

  public IdpGroup changeGroupMembership(
      String groupName, @Nullable List<String> usersToAdd, @Nullable List<String> usersToRemove) {
    String initiator = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(
        new IdpChangeGroupMembershipPreEvent(initiator, groupName, usersToAdd, usersToRemove));
    try {
      IdpGroup group = manager.changeGroupMembership(groupName, usersToAdd, usersToRemove);
      eventBus.dispatchEvent(
          new IdpChangeGroupMembershipEvent(initiator, groupName, usersToAdd, usersToRemove));
      return group;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IdpChangeGroupMembershipFailureEvent(
              initiator, groupName, usersToAdd, usersToRemove, e));
      throw e;
    }
  }
}
