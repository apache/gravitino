/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.*;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import javax.ws.rs.container.ContainerRequestContext;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestAdminFilter {

    @Test
    void testAdminFilterAllow() throws IOException  {
        AdminFilter filter = new AdminFilter();
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
        AccessControlManager accessControlManager = mock(AccessControlManager.class);
        GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
        RoleEntity roleEntity = mock(RoleEntity.class);
        when(accessControlManager.getRolesByUserFromMetalake(any(), any())).thenReturn(Lists.newArrayList(roleEntity));
        when(roleEntity.hasPrivilegeWithCondition(SecurableObjects.ofMetalake(Entity.SYSTEM_METALAKE_RESERVED_NAME, Lists.newArrayList(Privileges.AddUser.allow())), Privilege.Name.ADD_USER, Privilege.Condition.ALLOW)).thenReturn(true);
        when(roleEntity.hasPrivilegeWithCondition(SecurableObjects.ofMetalake(Entity.SYSTEM_METALAKE_RESERVED_NAME, Lists.newArrayList(Privileges.AddUser.allow())), Privilege.Name.ADD_USER, Privilege.Condition.DENY)).thenReturn(false);
        filter.filter(requestContext);
        verify(requestContext, never()).abortWith(any());

        when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
        GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
        filter.filter(requestContext);
        verify(requestContext, never()).abortWith(any());
    }

    @Test
    void testAdminFilterForbidden() throws IOException {
        AdminFilter filter = new AdminFilter();
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getMethod()).thenReturn(BasedRoleFilter.POST);
        AccessControlManager accessControlManager = mock(AccessControlManager.class);
        GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
        filter.filter(requestContext);
        verify(requestContext).abortWith(any());

        when(requestContext.getMethod()).thenReturn(BasedRoleFilter.DELETE);
    }
}
