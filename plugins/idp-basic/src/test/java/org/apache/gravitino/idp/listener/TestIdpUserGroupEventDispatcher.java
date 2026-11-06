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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.listener.event.IdpAddGroupEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpAddUserPreEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeGroupMembershipEvent;
import org.apache.gravitino.idp.listener.event.IdpChangeUserPasswordEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveGroupEvent;
import org.apache.gravitino.idp.listener.event.IdpRemoveUserEvent;
import org.apache.gravitino.idp.listener.event.IdpUpdateUserEnabledEvent;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIdpUserGroupEventDispatcher {

  private final List<Event> postEvents = new ArrayList<>();
  private final List<PreEvent> preEvents = new ArrayList<>();
  private IdpUserGroupManager manager;
  private IdpUserGroupEventDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    postEvents.clear();
    preEvents.clear();
    manager = mock(IdpUserGroupManager.class);
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    doAnswer(
            inv -> {
              postEvents.add(inv.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any(Event.class));
    doAnswer(
            inv -> {
              preEvents.add(inv.getArgument(0));
              return null;
            })
        .when(listener)
        .onPreEvent(any(PreEvent.class));
    dispatcher = new IdpUserGroupEventDispatcher(new EventBus(List.of(listener)), manager);
  }

  @Test
  void testUserMutationsDispatchTypedEvents() throws Exception {
    when(manager.addUser("alice", "Passw0rd-For-User", true)).thenReturn(mock(IdpUser.class));
    when(manager.changePassword("alice", "Passw0rd-For-User-2")).thenReturn(true);
    when(manager.updateEnabled("alice", false)).thenReturn(true);
    when(manager.updateEnabled("alice", true)).thenReturn(true);
    when(manager.removeUser("alice")).thenReturn(true);

    asAdmin(
        () -> {
          dispatcher.addUser("alice", "Passw0rd-For-User", true);
          dispatcher.changePassword("alice", "Passw0rd-For-User-2");
          dispatcher.updateEnabled("alice", false);
          dispatcher.updateEnabled("alice", true);
          dispatcher.removeUser("alice");
          return null;
        });

    Assertions.assertInstanceOf(IdpAddUserPreEvent.class, preEvents.get(0));
    Assertions.assertEquals(5, postEvents.size());
    IdpAddUserEvent add = (IdpAddUserEvent) postEvents.get(0);
    Assertions.assertEquals(OperationType.ADD_USER, add.operationType());
    Assertions.assertEquals("basic.user.alice", add.identifier().toString());
    Assertions.assertEquals("admin", add.user());
    Assertions.assertInstanceOf(IdpChangeUserPasswordEvent.class, postEvents.get(1));
    Assertions.assertEquals(OperationType.ALTER_USER, postEvents.get(1).operationType());
    Assertions.assertEquals(
        OperationType.DISABLE_USER,
        ((IdpUpdateUserEnabledEvent) postEvents.get(2)).operationType());
    Assertions.assertEquals(
        OperationType.ENABLE_USER, ((IdpUpdateUserEnabledEvent) postEvents.get(3)).operationType());
    IdpRemoveUserEvent remove = (IdpRemoveUserEvent) postEvents.get(4);
    Assertions.assertEquals(OperationType.REMOVE_USER, remove.operationType());
    Assertions.assertTrue(remove.isExists());
  }

  @Test
  void testGroupMutationsDispatchTypedEvents() throws Exception {
    when(manager.addGroup("g1", "c")).thenReturn(mock(IdpGroup.class));
    when(manager.changeGroupMembership(eq("g1"), anyList(), isNull()))
        .thenReturn(mock(IdpGroup.class));
    when(manager.removeGroup("g1", true)).thenReturn(true);

    asAdmin(
        () -> {
          dispatcher.addGroup("g1", "c");
          dispatcher.changeGroupMembership("g1", List.of("alice"), null);
          dispatcher.removeGroup("g1", true);
          return null;
        });

    Assertions.assertEquals(3, postEvents.size());
    Assertions.assertInstanceOf(IdpAddGroupEvent.class, postEvents.get(0));
    IdpChangeGroupMembershipEvent membership = (IdpChangeGroupMembershipEvent) postEvents.get(1);
    Assertions.assertEquals(OperationType.ALTER_GROUP, membership.operationType());
    Assertions.assertEquals(List.of("alice"), membership.usersToAdd());
    Assertions.assertEquals("basic.group.g1", membership.identifier().toString());
    IdpRemoveGroupEvent remove = (IdpRemoveGroupEvent) postEvents.get(2);
    Assertions.assertEquals(OperationType.REMOVE_GROUP, remove.operationType());
    Assertions.assertTrue(remove.force());
  }

  @Test
  void testFailureDispatchesFailureEvent() throws Exception {
    when(manager.addUser(eq("alice"), anyString(), eq(true)))
        .thenThrow(new AlreadyExistsException("exists"));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            asAdmin(
                () -> {
                  dispatcher.addUser("alice", "Passw0rd-For-User", true);
                  return null;
                }));

    Assertions.assertInstanceOf(IdpAddUserFailureEvent.class, postEvents.get(0));
    Assertions.assertEquals(OperationType.ADD_USER, postEvents.get(0).operationType());
  }

  private static void asAdmin(PrivilegedExceptionAction<Void> action) throws Exception {
    PrincipalUtils.doAs(new UserPrincipal("admin"), action);
  }
}
