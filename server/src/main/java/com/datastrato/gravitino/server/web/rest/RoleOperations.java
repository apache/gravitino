/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.responses.DeleteResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/roles")
public class RoleOperations {
  private static final Logger LOG = LoggerFactory.getLogger(RoleOperations.class);

  private static final String PRIVILEGE_ERROR_MSG = "%s shouldn't bind the privilege %s";
  private static final String UNSUPPORTED_ERROR_MSG = "%s uses an unsupported catalog type";
  private static final List<Privilege> CATALOG_PRIVILEGES =
      Lists.newArrayList(
          Privileges.LoadCatalog.get(),
          Privileges.AlterCatalog.get(),
          Privileges.CreateCatalog.get(),
          Privileges.DropCatalog.get());
  private static final List<Privilege> SCHEMA_PRIVILEGES =
      Lists.newArrayList(
          Privileges.LoadSchema.get(),
          Privileges.AlterSchema.get(),
          Privileges.CreateSchema.get(),
          Privileges.DropSchema.get());
  private static final List<Privilege> TABLE_PRIVILEGES =
      Lists.newArrayList(
          Privileges.LoadTable.get(),
          Privileges.AlterTable.get(),
          Privileges.CreateTable.get(),
          Privileges.DropTable.get(),
          Privileges.ReadTable.get(),
          Privileges.WriteTable.get());
  private static final List<Privilege> FILESET_PRIVILEGES =
      Lists.newArrayList(
          Privileges.LoadFileset.get(),
          Privileges.AlterFileset.get(),
          Privileges.CreateFileset.get(),
          Privileges.DropFileset.get(),
          Privileges.ReadFileset.get(),
          Privileges.WriteFileset.get());
  private static final List<Privilege> TOPIC_PRIVILEGES =
      Lists.newArrayList(
          Privileges.LoadTopic.get(),
          Privileges.AlterTopic.get(),
          Privileges.CreateTopic.get(),
          Privileges.DropTopic.get(),
          Privileges.ReadTopic.get(),
          Privileges.WriteTopic.get());

  private final AccessControlManager accessControlManager;
  private final CatalogManager catalogManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public RoleOperations(CatalogManager catalogManager) {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
    this.catalogManager = catalogManager;
  }

  @GET
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-role", absolute = true)
  public Response getRole(@PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RoleResponse(
                      DTOConverters.toDTO(accessControlManager.getRole(metalake, role)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.GET, role, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-role", absolute = true)
  public Response createRole(@PathParam("metalake") String metalake, RoleCreateRequest request) {
    try {
      checkSecurableObjectPrivileges(metalake, request);

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RoleResponse(
                      DTOConverters.toDTO(
                          accessControlManager.createRole(
                              metalake,
                              request.getName(),
                              request.getProperties(),
                              SecurableObjects.parse(request.getSecurableObject()),
                              request.getPrivileges().stream()
                                  .map(Privileges::fromString)
                                  .collect(Collectors.toList()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  /**
   * If you want to use the list operation, you should have the privilege of all the entities. So
   * only `*` has the privilege to list catalogs. Catalog has the privilege to list schemas. You can
   * choose two levels privilege: own level and children level. For example, one schema can be bind
   * the privilege to load a schema for own level, it can be bind the privilege to load a table at
   * the same time. But for `*`, it can't bind the privilege of schemas. Because Gravitino avoids
   * granting too many privileges to `*`.
   */
  @VisibleForTesting
  void checkSecurableObjectPrivileges(String metalake, RoleCreateRequest request) {
    SecurableObject securableObject = SecurableObjects.parse(request.getSecurableObject());
    List<Privilege> privileges =
        request.getPrivileges().stream().map(Privileges::fromString).collect(Collectors.toList());
    if (securableObject.parent() == null) {
      if ("*".equals(securableObject.name())) {
        for (Privilege privilege : privileges) {
          if (!CATALOG_PRIVILEGES.contains(privilege)
              && !Privileges.ListCatalog.get().equals(privilege)) {
            throw new IllegalArgumentException(
                String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
          }
        }
      } else {
        for (Privilege privilege : privileges) {
          if (!CATALOG_PRIVILEGES.contains(privilege)
              && !Privileges.ListSchema.get().equals(privilege)
              && !SCHEMA_PRIVILEGES.contains(privilege)) {
            throw new IllegalArgumentException(
                String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
          }
        }
      }
    } else {
      NameIdentifier ident = NameIdentifier.ofCatalog(metalake, getCatalogName(securableObject));

      Catalog catalog =
          TreeLockUtils.doWithTreeLock(
              ident, LockType.READ, () -> catalogManager.loadCatalog(ident));

      Catalog.Type type = catalog.type();

      if (isSchema(securableObject)) {
        for (Privilege privilege : privileges) {
          checkSchemaPrivileges(securableObject, type, privilege);
        }

      } else {
        for (Privilege privilege : privileges) {
          checkEntityPrivileges(securableObject, type, privilege);
        }
      }
    }
  }

  private static void checkSchemaPrivileges(
      SecurableObject securableObject, Catalog.Type type, Privilege privilege) {
    switch (type) {
      case FILESET:
        if (!FILESET_PRIVILEGES.contains(privilege)
            && !Privileges.ListFileset.get().equals(privilege)
            && !SCHEMA_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;

      case RELATIONAL:
        if (!TABLE_PRIVILEGES.contains(privilege)
            && !Privileges.ListTable.get().equals(privilege)
            && !SCHEMA_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;

      case MESSAGING:
        if (!TOPIC_PRIVILEGES.contains(privilege)
            && !Privileges.ListTopic.get().equals(privilege)
            && !SCHEMA_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;

      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, securableObject));
    }
  }

  private static void checkEntityPrivileges(
      SecurableObject securableObject, Catalog.Type type, Privilege privilege) {
    switch (type) {
      case FILESET:
        if (!FILESET_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;
      case RELATIONAL:
        if (!TABLE_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;
      case MESSAGING:
        if (!TOPIC_PRIVILEGES.contains(privilege)) {
          throw new IllegalArgumentException(
              String.format(PRIVILEGE_ERROR_MSG, securableObject, privilege));
        }
        break;
      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, securableObject));
    }
  }

  private static boolean isSchema(SecurableObject securableObject) {
    return securableObject.parent().parent() == null;
  }

  private static String getCatalogName(SecurableObject securableObject) {
    if (securableObject.parent() == null) {
      return securableObject.name();
    }

    return getCatalogName(securableObject.parent());
  }

  @DELETE
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-role", absolute = true)
  public Response deleteRole(
      @PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deteted = accessControlManager.deleteRole(metalake, role);
            if (!deteted) {
              LOG.warn("Failed to delete role {} under metalake {}", role, metalake);
            }
            return Utils.ok(new DeleteResponse(deteted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.DELETE, role, metalake, e);
    }
  }
}
