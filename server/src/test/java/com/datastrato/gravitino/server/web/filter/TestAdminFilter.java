/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.Lists;
import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestAdminFilter {

  @Test
  void testAdminFilterAllow() throws IOException {
    AdminFilter filter = new AdminFilter();
    ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);

    // Add metalake admin
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
    AccessControlManager accessControlManager = Mockito.mock(AccessControlManager.class);
    GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(
            roleEntity.hasPrivilegeWithCondition(
                SecurableObjects.ofMetalake(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Lists.newArrayList(Privileges.AddUser.allow())),
                Privilege.Name.ADD_USER,
                Privilege.Condition.ALLOW))
        .thenReturn(true);
    Mockito.when(
            roleEntity.hasPrivilegeWithCondition(
                SecurableObjects.ofMetalake(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Lists.newArrayList(Privileges.AddUser.allow())),
                Privilege.Name.ADD_USER,
                Privilege.Condition.DENY))
        .thenReturn(false);
    filter.filter(requestContext);
    Mockito.verify(requestContext, Mockito.never()).abortWith(any());

    // Remove metalake admin
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
    GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(
            roleEntity.hasPrivilegeWithCondition(
                SecurableObjects.ofMetalake(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Lists.newArrayList(Privileges.RemoveUser.allow())),
                Privilege.Name.REMOVE_USER,
                Privilege.Condition.ALLOW))
        .thenReturn(true);
    Mockito.when(
            roleEntity.hasPrivilegeWithCondition(
                SecurableObjects.ofMetalake(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Lists.newArrayList(Privileges.RemoveUser.allow())),
                Privilege.Name.REMOVE_USER,
                Privilege.Condition.DENY))
        .thenReturn(false);
    filter.filter(requestContext);
    Mockito.verify(requestContext, Mockito.never()).abortWith(any());
  }

  @Test
  void testAdminFilterForbidden() throws IOException {
    AdminFilter filter = new AdminFilter();
    ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
    AccessControlManager accessControlManager = Mockito.mock(AccessControlManager.class);
    GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);

    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.hasPrivilegeWithCondition(any(), any(), any())).thenReturn(true);
    filter.filter(requestContext);
    Mockito.verify(requestContext).abortWith(any());

    // Remove metalake admin
    Mockito.reset(requestContext);
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
    GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.hasPrivilegeWithCondition(any(), any(), any())).thenReturn(true);
    filter.filter(requestContext);
    Mockito.verify(requestContext).abortWith(any());
  }
}
