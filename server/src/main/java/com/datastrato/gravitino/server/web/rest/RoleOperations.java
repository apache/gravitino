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
import com.google.common.collect.ImmutableList;
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

  private static final String UNSUPPORTED_ERROR_MSG = "%s is an unsupported catalog type";
  private static final List<Privilege> CATALOG_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.LoadCatalog.get(),
          Privileges.LoadCatalog.get(),
          Privileges.AlterCatalog.get(),
          Privileges.CreateCatalog.get(),
          Privileges.DropCatalog.get());
  private static final List<Privilege> CATALOGS_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(CATALOG_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListCatalog.get())
          .build();
  private static final List<Privilege> SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.LoadSchema.get(),
          Privileges.AlterSchema.get(),
          Privileges.CreateSchema.get(),
          Privileges.DropSchema.get());
  private static final List<Privilege> SCHEMA_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListSchema.get())
          .build();
  private static final List<Privilege> TABLE_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterTable.get(),
          Privileges.CreateTable.get(),
          Privileges.DropTable.get(),
          Privileges.ReadTable.get(),
          Privileges.WriteTable.get());
  private static final List<Privilege> TABLE_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(TABLE_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListTable.get())
          .build();
  private static final List<Privilege> FILESET_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterFileset.get(),
          Privileges.CreateFileset.get(),
          Privileges.DropFileset.get(),
          Privileges.ReadFileset.get(),
          Privileges.WriteFileset.get());
  private static final List<Privilege> FILESET_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(FILESET_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListFileset.get())
          .build();
  private static final List<Privilege> TOPIC_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterTopic.get(),
          Privileges.CreateTopic.get(),
          Privileges.DropTopic.get(),
          Privileges.ReadTopic.get(),
          Privileges.WriteTopic.get());
  private static final List<Privilege> TOPIC_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(TOPIC_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListTopic.get())
          .build();

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
   * * There are 6 kinds of entities for the privileges: <br>
   * `*` has all the catalog operation privileges. <br>
   * `catalog` has the catalog operation privileges except for list operation and all the schema
   * operation privileges. <br>
   * `catalog.schema` has the schema operation privileges except for list operation and all the
   * table/topic/fileset privileges. <br>
   * `catalog.schema.table` has the table operation privileges except for list operation. <br>
   * `catalog.schema.topic` has the topic operation privileges except for list operation. <br>
   * `catalog.schema.fileset` has the fileset operation privileges except for list operation. <br>
   * The entity can only add the children entity privilege of list operation.
   */
  @VisibleForTesting
  void checkSecurableObjectPrivileges(String metalake, RoleCreateRequest request) {
    SecurableObject securableObject = SecurableObjects.parse(request.getSecurableObject());
    List<Privilege> privileges =
        request.getPrivileges().stream().map(Privileges::fromString).collect(Collectors.toList());
    List<Privilege> supportedPrivileges = getEntitySupportedPrivileges(metalake, securableObject);
    for (Privilege privilege : privileges) {
      if (!supportedPrivileges.contains(privilege)) {
        throw new IllegalArgumentException(
            String.format("%s shouldn't bind the privilege %s", securableObject, privilege));
      }
    }
  }

  private List<Privilege> getEntitySupportedPrivileges(
      String metalake, SecurableObject securableObject) {
    if (isAllCatalogs(securableObject)) {
      return ImmutableList.<Privilege>builder().addAll(CATALOGS_PRIVILEGES).build();
    }

    if (isSingleCatalog(securableObject)) {
      return ImmutableList.<Privilege>builder()
          .addAll(CATALOG_PRIVILEGES_EXCEPT_FOR_LIST)
          .addAll(SCHEMA_PRIVILEGES)
          .build();
    }

    NameIdentifier ident = NameIdentifier.ofCatalog(metalake, getCatalogName(securableObject));

    Catalog catalog =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> catalogManager.loadCatalog(ident));

    Catalog.Type type = catalog.type();

    if (isSchema(securableObject)) {
      return ImmutableList.<Privilege>builder()
          .addAll(SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST)
          .addAll(getLeavesEntityPrivileges(type))
          .build();
    }

    return getLeavesEntityPrivilegesExceptForList(type);
  }

  private List<Privilege> getLeavesEntityPrivileges(Catalog.Type type) {
    switch (type) {
      case FILESET:
        return FILESET_PRIVILEGES;

      case RELATIONAL:
        return TABLE_PRIVILEGES;

      case MESSAGING:
        return TOPIC_PRIVILEGES;

      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, type));
    }
  }

  private List<Privilege> getLeavesEntityPrivilegesExceptForList(Catalog.Type type) {
    switch (type) {
      case FILESET:
        return FILESET_PRIVILEGES_EXCEPT_FOR_LIST;

      case RELATIONAL:
        return TABLE_PRIVILEGES_EXCEPT_FOR_LIST;

      case MESSAGING:
        return TOPIC_PRIVILEGES_EXCEPT_FOR_LIST;

      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, type));
    }
  }

  private boolean isAllCatalogs(SecurableObject securableObject) {
    if (securableObject.parent() == null && "*".equals(securableObject.name())) {
      return true;
    }

    return false;
  }

  private boolean isSingleCatalog(SecurableObject securableObject) {
    if (securableObject.parent() == null && !"*".equals(securableObject.name())) {
      return true;
    }

    return false;
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
