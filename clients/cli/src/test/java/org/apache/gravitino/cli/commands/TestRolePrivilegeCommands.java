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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.client.GravitinoClient;
import org.junit.jupiter.api.Test;

class TestRolePrivilegeCommands {
  @Test
  void testGrantAndRevokeGlossaryPrivilegesOnMetalake() {
    CommandLine line = mock(CommandLine.class);
    when(line.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(line.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("lake");
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
}
