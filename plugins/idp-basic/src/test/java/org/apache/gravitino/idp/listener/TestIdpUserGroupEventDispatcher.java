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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.listener.event.IdpAddGroupEvent;
import org.apache.gravitino.idp.listener.event.IdpAddGroupFailureEvent;
import org.apache.gravitino.idp.listener.event.IdpAddGroupPreEvent;
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

  private RecordingListener listener;
  private IdpUserGroupManager manager;
  private IdpUserGroupEventDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    listener = new RecordingListener();
    manager = mock(IdpUserGroupManager.class);
    dispatcher = new IdpUserGroupEventDispatcher(new EventBus(List.of(listener)), manager);
  }

  @Test
  void testAddUserDispatchesTypedEvents() throws Exception {
    when(manager.addUser("alice", "Passw0rd-For-User", true)).thenReturn(mock(IdpUser.class));

    PrincipalUtils.doAs(
        new UserPrincipal("admin"),
        () -> {
          dispatcher.addUser("alice", "Passw0rd-For-User", true);
          return null;
        });

    Assertions.assertEquals(1, listener.preEvents.size());
    Assertions.assertInstanceOf(IdpAddUserPreEvent.class, listener.preEvents.get(0));
    Assertions.assertEquals(1, listener.postEvents.size());
    IdpAddUserEvent event = (IdpAddUserEvent) listener.postEvents.get(0);
    Assertions.assertEquals(OperationType.ADD_USER, event.operationType());
    Assertions.assertEquals("basic.user.alice", event.identifier().toString());
    Assertions.assertEquals("admin", event.user());
  }

  @Test
  void testAddUserFailureDispatchesFailureEvent() throws Exception {
    when(manager.addUser(eq("alice"), anyString(), anyBoolean()))
        .thenThrow(new AlreadyExistsException("exists"));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            PrincipalUtils.doAs(
                new UserPrincipal("admin"),
                () -> {
                  dispatcher.addUser("alice", "Passw0rd-For-User", true);
                  return null;
                }));

    Assertions.assertEquals(1, listener.preEvents.size());
    Assertions.assertEquals(1, listener.postEvents.size());
    Assertions.assertInstanceOf(IdpAddUserFailureEvent.class, listener.postEvents.get(0));
    Assertions.assertEquals(OperationType.ADD_USER, listener.postEvents.get(0).operationType());
  }

  @Test
  void testUpdateEnabledUsesEnableAndDisableOperationTypes() throws Exception {
    when(manager.updateEnabled("alice", false)).thenReturn(true);
    when(manager.updateEnabled("alice", true)).thenReturn(true);

    PrincipalUtils.doAs(
        new UserPrincipal("admin"),
        () -> {
          dispatcher.updateEnabled("alice", false);
          dispatcher.updateEnabled("alice", true);
          return null;
        });

    Assertions.assertEquals(2, listener.postEvents.size());
    IdpUpdateUserEnabledEvent disable = (IdpUpdateUserEnabledEvent) listener.postEvents.get(0);
    IdpUpdateUserEnabledEvent enable = (IdpUpdateUserEnabledEvent) listener.postEvents.get(1);
    Assertions.assertEquals(OperationType.DISABLE_USER, disable.operationType());
    Assertions.assertEquals(OperationType.ENABLE_USER, enable.operationType());
    Assertions.assertEquals("basic.user.alice", disable.identifier().toString());
  }

  @Test
  void testChangePasswordAndRemoveUser() throws Exception {
    when(manager.changePassword("alice", "Passw0rd-For-User-2")).thenReturn(true);
    when(manager.removeUser("alice")).thenReturn(true);

    PrincipalUtils.doAs(
        new UserPrincipal("admin"),
        () -> {
          dispatcher.changePassword("alice", "Passw0rd-For-User-2");
          dispatcher.removeUser("alice");
          return null;
        });

    Assertions.assertEquals(2, listener.postEvents.size());
    Assertions.assertInstanceOf(IdpChangeUserPasswordEvent.class, listener.postEvents.get(0));
    Assertions.assertEquals(OperationType.ALTER_USER, listener.postEvents.get(0).operationType());
    IdpRemoveUserEvent remove = (IdpRemoveUserEvent) listener.postEvents.get(1);
    Assertions.assertEquals(OperationType.REMOVE_USER, remove.operationType());
    Assertions.assertTrue(remove.isExists());
  }

  @Test
  void testGroupMutations() throws Exception {
    when(manager.addGroup("g1", "c")).thenReturn(mock(IdpGroup.class));
    when(manager.changeGroupMembership(eq("g1"), anyList(), isNull()))
        .thenReturn(mock(IdpGroup.class));
    when(manager.removeGroup("g1", true)).thenReturn(true);

    PrincipalUtils.doAs(
        new UserPrincipal("admin"),
        () -> {
          dispatcher.addGroup("g1", "c");
          dispatcher.changeGroupMembership("g1", List.of("alice"), null);
          dispatcher.removeGroup("g1", true);
          return null;
        });

    Assertions.assertEquals(3, listener.preEvents.size());
    Assertions.assertInstanceOf(IdpAddGroupPreEvent.class, listener.preEvents.get(0));
    Assertions.assertEquals(3, listener.postEvents.size());
    Assertions.assertInstanceOf(IdpAddGroupEvent.class, listener.postEvents.get(0));
    IdpChangeGroupMembershipEvent membership =
        (IdpChangeGroupMembershipEvent) listener.postEvents.get(1);
    Assertions.assertEquals(OperationType.ALTER_GROUP, membership.operationType());
    Assertions.assertEquals(List.of("alice"), membership.usersToAdd());
    Assertions.assertEquals("basic.group.g1", membership.identifier().toString());
    IdpRemoveGroupEvent remove = (IdpRemoveGroupEvent) listener.postEvents.get(2);
    Assertions.assertEquals(OperationType.REMOVE_GROUP, remove.operationType());
    Assertions.assertTrue(remove.force());
  }

  @Test
  void testAddGroupFailure() throws Exception {
    doThrow(new AlreadyExistsException("exists")).when(manager).addGroup("g1", "");

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            PrincipalUtils.doAs(
                new UserPrincipal("admin"),
                () -> {
                  dispatcher.addGroup("g1", "");
                  return null;
                }));

    Assertions.assertInstanceOf(IdpAddGroupFailureEvent.class, listener.postEvents.get(0));
  }

  private static final class RecordingListener implements EventListenerPlugin {
    private final LinkedList<Event> postEvents = new LinkedList<>();
    private final LinkedList<PreEvent> preEvents = new LinkedList<>();

    @Override
    public void init(Map<String, String> properties) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void onPostEvent(Event event) {
      postEvents.add(event);
    }

    @Override
    public void onPreEvent(PreEvent preEvent) {
      preEvents.add(preEvent);
    }
  }
}
