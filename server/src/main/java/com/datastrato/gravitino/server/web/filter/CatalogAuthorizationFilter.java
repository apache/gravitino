/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.*;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.server.authorization.NameBindings;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.security.Principal;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;

@Provider
@NameBindings.CatalogInterfaces
public class CatalogAuthorizationFilter extends BaseMetalakeUserFilter {

    private MetalakeManager metalakeManager = GravitinoEnv.getInstance().metalakesManager();
    private CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {

        if (!checkUserInMetalake(requestContext)) {
            requestContext.abortWith(
                    Response.status(
                                    SC_FORBIDDEN, "Only the users in the metalake can execute the catalog operations")
                            .build());
            return;
        }

        String method = requestContext.getMethod();
        String metalake = requestContext.getUriInfo().getPathParameters().getFirst("metalake");
        String catalog = requestContext.getUriInfo().getPathParameters().getFirst("catalog");




        Principal principal =
                (Principal) httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
        try {
            if (isCreator(metalake, catalog, principal.getName())) {
                return;
            }
        } catch (NoSuchMetalakeException nsm) {
            return;
        } catch (NoSuchCatalogException nsc) {
            return;
        }

        Privilege privilege = getPrivilege(catalog, method);
        NameIdentifier ident = getIdentifier(metalake, catalog);

        User user = accessControlManager.getUser(metalake, principal.getName());
        for (String role : user.roles()) {
            Role detailedRole = accessControlManager.loadRole(metalake, role);
            if (detailedRole.privileges().contains(privilege)) {
                if (detailedRole.privilegeEntityIdentifier().equals(ident) || detailedRole.privilegeEntityIdentifier().equals(ident)) {
                    return;
                }
            }
        }

        for (String group : accessControlManager.getGroupsByUser(principal.getName())) {
            try {
                Group detailedGroup = accessControlManager.getGroup(metalake, group);
                for (String role : detailedGroup.roles()) {
                    Role detailedRole = accessControlManager.loadRole(metalake, role);
                    if (detailedRole.privileges().contains(privilege)) {
                        if (detailedRole.privilegeEntityIdentifier().equals(ident) || detailedRole.privilegeEntityIdentifier().equals(ident)) {
                            return;
                        }
                    }
                }
            } catch (NoSuchGroupException nsg) {
                // ignore
            }
        }
    }

    private boolean isCreator(String metalake, String catalog, String user) {
        if (catalog == null) {
            return metalakeManager.loadMetalake(NameIdentifier.ofMetalake(metalake)).auditInfo().creator().equals(user);
        }
        return catalogManager.loadCatalog(NameIdentifier.ofCatalog(metalake, catalog)).auditInfo().creator().equals(user);
    }

    private NameIdentifier getIdentifier(String metalake, String catalog) {
        if (catalog == null) {
            return NameIdentifier.ofMetalake(metalake);
        }

        return NameIdentifier.ofCatalog(metalake, catalog);
    }

    Privilege getPrivilege(String catalog, String method) {
        if (catalog == null && HttpMethod.GET.equals(method)) {
            return Privileges.LoadCatalog.get();
        }

        if (HttpMethod.POST.equals(method)) {

        }

        if (catalog == null) {
           throw new IllegalStateException("");
        }

        if (HttpMethod.GET.equals(method)) {
            return Privileges.LoadCatalog.get();
        }

        if (HttpMethod.PUT.equals(method)) {

        }

        if (HttpMethod.DELETE.equals(method)) {

        }

        throw new IllegalStateException("");
    }
}
