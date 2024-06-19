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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeAdminFilter {

  @Test
  void testAdminFilterAllow() throws IOException, IllegalAccessException {
    MetalakeAdminFilter filter = new MetalakeAdminFilter();
    ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);

    // Add metalake admin
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
    AccessControlManager accessControlManager = Mockito.mock(AccessControlManager.class);
    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlManager", accessControlManager, true);
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
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlManager", accessControlManager, true);
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
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
  void testAdminFilterDeny() throws IOException, IllegalAccessException {
    MetalakeAdminFilter filter = new MetalakeAdminFilter();
    ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
    AccessControlManager accessControlManager = Mockito.mock(AccessControlManager.class);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlManager", accessControlManager, true);
    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.hasPrivilegeWithCondition(any(), any(), any())).thenReturn(true);
    filter.filter(requestContext);
    Mockito.verify(requestContext).abortWith(any());

    // Remove metalake admin
    Mockito.reset(requestContext);
    Mockito.when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlManager", accessControlManager, true);
    Mockito.when(accessControlManager.getRolesByUserFromMetalake(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.hasPrivilegeWithCondition(any(), any(), any())).thenReturn(true);
    filter.filter(requestContext);
    Mockito.verify(requestContext).abortWith(any());
  }
}
