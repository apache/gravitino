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

package org.apache.gravitino.cli.commands;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRolePrivilegeCommands {
  @Test
  void testGrantAndRevokeGlossaryPrivilegesOnMetalake() throws ParseException {
    CommandLine line = parse("--metalake", "lake");
    CommandContext context = new CommandContext(line);
    FullName target = new FullName(line);
    String[] arguments = {"view_definition", "create_definition", "manage_definitions"};
    GravitinoClient client = mock(GravitinoClient.class);
    GrantPrivilegesToRole grant =
        spy(new GrantPrivilegesToRole(context, "lake", "editor", target, arguments));
    RevokePrivilegesFromRole revoke =
        spy(new RevokePrivilegesFromRole(context, "lake", "editor", target, arguments));
    doReturn(client).when(grant).buildClient("lake");
    doReturn(client).when(revoke).buildClient("lake");

    grant.validate().handle();
    revoke.validate().handle();

    MetadataObject metalake = MetadataObjects.of(null, "lake", MetadataObject.Type.METALAKE);
    Set<Privilege> privileges =
        Set.of(
            Privileges.allow("VIEW_DEFINITION"),
            Privileges.allow("CREATE_DEFINITION"),
            Privileges.allow("MANAGE_DEFINITIONS"));
    verify(client).grantPrivilegesToRole("editor", metalake, privileges);
    Set<Privilege> revoked = new HashSet<>(privileges);
    privileges.forEach(privilege -> revoked.add(Privileges.deny(privilege.name())));
    verify(client).revokePrivilegesFromRole("editor", metalake, revoked);
  }

  @ParameterizedTest
  @ValueSource(strings = {".", ".."})
  void testExplicitMalformedTargetCannotChangeMetalakePrivileges(String name)
      throws ParseException {
    CommandLine line = parse("--metalake", "lake", "--name", name);
    CommandContext context = new CommandContext(line);
    FullName target = new FullName(line);
    String[] arguments = {"view_definition"};
    GravitinoClient client = mock(GravitinoClient.class);
    GrantPrivilegesToRole grant =
        spy(new GrantPrivilegesToRole(context, "lake", "editor", target, arguments));
    RevokePrivilegesFromRole revoke =
        spy(new RevokePrivilegesFromRole(context, "lake", "editor", target, arguments));
    doReturn(client).when(grant).buildClient("lake");
    doReturn(client).when(revoke).buildClient("lake");
    doNothing().when(grant).exitWithError(anyString());
    doNothing().when(revoke).exitWithError(anyString());

    grant.validate().handle();
    revoke.validate().handle();

    verify(grant).exitWithError("Unknown entity type: " + name);
    verify(revoke).exitWithError("Unknown entity type: " + name);
    verifyNoInteractions(client);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRevokeAllSelectsCatalogWhenMetalakeHasTheSameName(boolean hasCatalogGrant)
      throws ParseException {
    CommandLine line = parse("--metalake", "lake", "--name", "lake", "--all");
    RevokeAllPrivileges revoke =
        spy(
            new RevokeAllPrivileges(
                new CommandContext(line), "lake", "editor", new FullName(line)));
    GravitinoClient client = mock(GravitinoClient.class);
    doReturn(client).when(revoke).buildClient("lake");
    Role role = mock(Role.class);
    when(client.getRole("editor")).thenReturn(role);
    Privilege catalogGrant = Privileges.allow("USE_CATALOG");
    SecurableObject lake =
        SecurableObjects.ofMetalake("lake", List.of(Privileges.allow("VIEW_DEFINITION")));
    SecurableObject catalog = SecurableObjects.ofCatalog("lake", List.of(catalogGrant));
    when(role.securableObjects())
        .thenReturn(hasCatalogGrant ? List.of(lake, catalog) : List.of(lake));

    revoke.validate().handle();

    verify(client).getRole("editor");
    verify(client, times(hasCatalogGrant ? 1 : 0))
        .revokePrivilegesFromRole(
            "editor",
            MetadataObjects.of(null, "lake", MetadataObject.Type.CATALOG),
            Set.of(catalogGrant));
    verifyNoMoreInteractions(client);
  }

  @Test
  void testGrantReportsMissingTarget() throws ParseException {
    CommandLine line = parse("--metalake", "lake", "--name", "catalog");
    GrantPrivilegesToRole grant =
        spy(
            new GrantPrivilegesToRole(
                new CommandContext(line),
                "lake",
                "editor",
                new FullName(line),
                new String[] {"use_catalog"}));
    GravitinoClient client = mock(GravitinoClient.class);
    doReturn(client).when(grant).buildClient("lake");
    doNothing().when(grant).exitWithError(anyString());
    doThrow(new NoSuchMetadataObjectException("Missing catalog"))
        .when(client)
        .grantPrivilegesToRole(anyString(), any(MetadataObject.class), anySet());

    grant.validate().handle();

    verify(grant).exitWithError(ErrorMessages.UNKNOWN_ENTITY);
  }

  private static CommandLine parse(String... args) throws ParseException {
    return new DefaultParser().parse(new GravitinoOptions().options(), args);
  }
}
