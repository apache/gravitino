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
package org.apache.gravitino.server.web.rest;

import com.google.common.annotations.VisibleForTesting;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptyMetalakeException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.server.web.Utils;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandlers.class);

  private ExceptionHandlers() {}

  public static Response handlePartitionException(
      OperationType op, String partition, String table, Exception e) {
    return PartitionExceptionHandler.INSTANCE.handle(op, partition, table, e);
  }

  public static Response handleTableException(
      OperationType op, String table, String schema, Exception e) {
    return TableExceptionHandler.INSTANCE.handle(op, table, schema, e);
  }

  public static Response handleSchemaException(
      OperationType op, String schema, String catalog, Exception e) {
    return SchemaExceptionHandler.INSTANCE.handle(op, schema, catalog, e);
  }

  public static Response handleCatalogException(
      OperationType op, String catalog, String metalake, Exception e) {
    return CatalogExceptionHandler.INSTANCE.handle(op, catalog, metalake, e);
  }

  public static Response handleMetalakeException(OperationType op, String metalake, Exception e) {
    return MetalakeExceptionHandler.INSTANCE.handle(op, metalake, "", e);
  }

  public static Response handleFilesetException(
      OperationType op, String fileset, String schema, Exception e) {
    return FilesetExceptionHandler.INSTANCE.handle(op, fileset, schema, e);
  }

  public static Response handleUserException(
      OperationType op, String user, String metalake, Exception e) {
    return UserExceptionHandler.INSTANCE.handle(op, user, metalake, e);
  }

  public static Response handleGroupException(
      OperationType op, String group, String metalake, Exception e) {
    return GroupExceptionHandler.INSTANCE.handle(op, group, metalake, e);
  }

  public static Response handleRoleException(
      OperationType op, String role, String metalake, Exception e) {
    return RoleExceptionHandler.INSTANCE.handle(op, role, metalake, e);
  }

  public static Response handleTopicException(
      OperationType op, String topic, String schema, Exception e) {
    return TopicExceptionHandler.INSTANCE.handle(op, topic, schema, e);
  }

  public static Response handleUserPermissionOperationException(
      OperationType op, String roles, String parent, Exception e) {
    return UserPermissionOperationExceptionHandler.INSTANCE.handle(op, roles, parent, e);
  }

  public static Response handleGroupPermissionOperationException(
      OperationType op, String roles, String parent, Exception e) {
    return GroupPermissionOperationExceptionHandler.INSTANCE.handle(op, roles, parent, e);
  }

  public static Response handleTagException(
      OperationType op, String tag, String parent, Exception e) {
    return TagExceptionHandler.INSTANCE.handle(op, tag, parent, e);
  }

  public static Response handlePolicyException(
      OperationType op, String policy, String parent, Exception e) {
    return PolicyExceptionHandler.INSTANCE.handle(op, policy, parent, e);
  }

  public static Response handleCredentialException(
      OperationType op, String metadataObjectName, Exception e) {
    return CredentialExceptionHandler.INSTANCE.handle(op, metadataObjectName, "", e);
  }

  public static Response handleModelException(
      OperationType op, String model, String schema, Exception e) {
    return ModelExceptionHandler.INSTANCE.handle(op, model, schema, e);
  }

  public static Response handleJobTemplateException(
      OperationType op, String jobTemplate, String metalake, Exception e) {
    return JobTemplateExceptionHandler.INSTANCE.handle(op, jobTemplate, metalake, e);
  }

  public static Response handleJobException(
      OperationType op, String job, String metalake, Exception e) {
    return JobExceptionHandler.INSTANCE.handle(op, job, metalake, e);
  }

  public static Response handleTestConnectionException(Exception e) {
    ErrorResponse response;
    if (e instanceof IllegalArgumentException) {
      response = ErrorResponse.illegalArguments(e.getMessage(), e);

    } else if (e instanceof ConnectionFailedException) {
      response = ErrorResponse.connectionFailed(e.getMessage(), e);

    } else if (e instanceof NotFoundException) {
      response = ErrorResponse.notFound(e.getClass().getSimpleName(), e.getMessage(), e);

    } else if (e instanceof AlreadyExistsException) {
      response = ErrorResponse.alreadyExists(e.getClass().getSimpleName(), e.getMessage(), e);

    } else if (e instanceof NotInUseException) {
      response = ErrorResponse.notInUse(e.getClass().getSimpleName(), e.getMessage(), e);

    } else {
      return Utils.internalError(e.getMessage(), e);
    }

    return Response.status(Response.Status.OK)
        .entity(response)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response handleOwnerException(
      OperationType type, String name, String metalake, Exception e) {
    return OwnerExceptionHandler.INSTANCE.handle(type, name, metalake, e);
  }

  public static Response handleRolePermissionOperationException(
      OperationType type, String name, String parent, Exception e) {
    return RolePermissionOperationHandler.INSTANCE.handle(type, name, parent, e);
  }

  public static Response handleStatisticException(
      OperationType type, String name, String parent, Exception e) {
    return StatisticExceptionHandler.INSTANCE.handle(type, name, parent, e);
  }

  public static Response handlePartitionStatsException(
      OperationType type, String name, String parent, Exception e) {
    return PartitionStatsExceptionHandler.INSTANCE.handle(type, name, parent, e);
  }

  private static class PartitionExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new PartitionExceptionHandler();

    private static String getPartitionErrorMsg(
        String partition, String operation, String table, String reason) {
      return String.format(
          "Failed to operate partition(s)%s operation [%s] of table [%s], reason [%s]",
          partition, operation, table, reason);
    }

    @Override
    public Response handle(OperationType op, String partition, String table, Exception e) {
      String formatted = StringUtil.isBlank(partition) ? "" : " [" + partition + "]";
      String errorMsg = getPartitionErrorMsg(formatted, op.name(), table, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof PartitionAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, partition, table, e);
      }
    }
  }

  private static class TableExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new TableExceptionHandler();

    private static String getTableErrorMsg(
        String table, String operation, String schema, String reason) {
      return String.format(
          "Failed to operate table(s)%s operation [%s] under schema [%s], reason [%s]",
          table, operation, schema, reason);
    }

    @Override
    public Response handle(OperationType op, String table, String schema, Exception e) {
      String formatted = StringUtil.isBlank(table) ? "" : " [" + table + "]";
      String errorMsg = getTableErrorMsg(formatted, op.name(), schema, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof TableAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, table, schema, e);
      }
    }
  }

  private static class SchemaExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new SchemaExceptionHandler();

    private static String getSchemaErrorMsg(
        String schema, String operation, String catalog, String reason) {
      return String.format(
          "Failed to operate schema(s)%s operation [%s] under catalog [%s], reason [%s]",
          schema, operation, catalog, reason);
    }

    @Override
    public Response handle(OperationType op, String schema, String catalog, Exception e) {
      String formatted = StringUtil.isBlank(schema) ? "" : " [" + schema + "]";
      String errorMsg = getSchemaErrorMsg(formatted, op.name(), catalog, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof SchemaAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NonEmptySchemaException) {
        return Utils.nonEmpty(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, schema, catalog, e);
      }
    }
  }

  private static class CatalogExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new CatalogExceptionHandler();

    private static String getCatalogErrorMsg(
        String catalog, String operation, String metalake, String reason) {
      return String.format(
          "Failed to operate catalog(s)%s operation [%s] under metalake [%s], reason [%s]",
          catalog, operation, metalake, reason);
    }

    @Override
    public Response handle(OperationType op, String catalog, String metalake, Exception e) {
      String formatted = StringUtil.isBlank(catalog) ? "" : " [" + catalog + "]";
      String errorMsg = getCatalogErrorMsg(formatted, op.name(), metalake, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof ConnectionFailedException) {
        return Utils.connectionFailed(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof CatalogAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else if (e instanceof InUseException) {
        return Utils.inUse(errorMsg, e);

      } else if (e instanceof NonEmptyCatalogException) {
        return Utils.nonEmpty(errorMsg, e);

      } else {
        return super.handle(op, catalog, metalake, e);
      }
    }
  }

  private static class MetalakeExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new MetalakeExceptionHandler();

    private static String getMetalakeErrorMsg(String metalake, String operation, String reason) {
      return String.format(
          "Failed to operate metalake(s)%s operation [%s], reason [%s]",
          metalake, operation, reason);
    }

    @Override
    public Response handle(OperationType op, String metalake, String parent, Exception e) {
      String formatted = StringUtil.isBlank(metalake) ? "" : " [" + metalake + "]";
      String errorMsg = getMetalakeErrorMsg(formatted, op.name(), getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof MetalakeAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NoSuchMetalakeException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof MetalakeNotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else if (e instanceof MetalakeInUseException) {
        return Utils.inUse(errorMsg, e);

      } else if (e instanceof NonEmptyMetalakeException) {
        return Utils.nonEmpty(errorMsg, e);

      } else {
        return super.handle(op, metalake, parent, e);
      }
    }
  }

  private static class FilesetExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new FilesetExceptionHandler();

    private static String getFilesetErrorMsg(
        String fileset, String operation, String schema, String reason) {
      return String.format(
          "Failed to operate fileset(s)%s operation [%s] under schema [%s], reason [%s]",
          fileset, operation, schema, reason);
    }

    @Override
    public Response handle(OperationType op, String fileset, String schema, Exception e) {
      String formatted = StringUtil.isBlank(fileset) ? "" : " [" + fileset + "]";
      String errorMsg = getFilesetErrorMsg(formatted, op.name(), schema, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof FilesetAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, fileset, schema, e);
      }
    }
  }

  private static class UserExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new UserExceptionHandler();

    private static String getUserErrorMsg(
        String user, String operation, String metalake, String reason) {
      if (metalake == null) {
        return String.format(
            "Failed to operate metalake admin user %s operation [%s], reason [%s]",
            user, operation, reason);
      } else {
        return String.format(
            "Failed to operate user %s operation [%s] under metalake [%s], reason [%s]",
            user, operation, metalake, reason);
      }
    }

    @Override
    public Response handle(OperationType op, String user, String metalake, Exception e) {
      String formatted = StringUtil.isBlank(user) ? "" : " [" + user + "]";
      String errorMsg = getUserErrorMsg(formatted, op.name(), metalake, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof UserAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, user, metalake, e);
      }
    }
  }

  private static class GroupExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new GroupExceptionHandler();

    private static String getGroupErrorMsg(
        String group, String operation, String metalake, String reason) {
      return String.format(
          "Failed to operate group %s operation [%s] under metalake [%s], reason [%s]",
          group, operation, metalake, reason);
    }

    @Override
    public Response handle(OperationType op, String group, String metalake, Exception e) {
      String formatted = StringUtil.isBlank(group) ? "" : " [" + group + "]";
      String errorMsg = getGroupErrorMsg(formatted, op.name(), metalake, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof GroupAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, group, metalake, e);
      }
    }
  }

  private static class RoleExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new RoleExceptionHandler();

    private static String getRoleErrorMsg(
        String role, String operation, String metalake, String reason) {
      return String.format(
          "Failed to operate role %s operation [%s] under metalake [%s], reason [%s]",
          role, operation, metalake, reason);
    }

    @Override
    public Response handle(OperationType op, String role, String metalake, Exception e) {
      String formatted = StringUtil.isBlank(role) ? "" : " [" + role + "]";
      String errorMsg = getRoleErrorMsg(formatted, op.name(), metalake, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof RoleAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, role, metalake, e);
      }
    }
  }

  private static class TopicExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new TopicExceptionHandler();

    private static String getTopicErrorMsg(
        String topic, String operation, String schema, String reason) {
      return String.format(
          "Failed to operate topic(s)%s operation [%s] under schema [%s], reason [%s]",
          topic, operation, schema, reason);
    }

    @Override
    public Response handle(OperationType op, String topic, String schema, Exception e) {
      String formatted = StringUtil.isBlank(topic) ? "" : " [" + topic + "]";
      String errorMsg = getTopicErrorMsg(formatted, op.name(), schema, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof TopicAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, topic, schema, e);
      }
    }
  }

  private static class UserPermissionOperationExceptionHandler
      extends BasePermissionExceptionHandler {

    private static final ExceptionHandler INSTANCE = new UserPermissionOperationExceptionHandler();

    @Override
    protected String getPermissionErrorMsg(
        String role, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate role(s)%s operation [%s] under user [%s], reason [%s]",
          role, operation, parent, reason);
    }
  }

  private static class GroupPermissionOperationExceptionHandler
      extends BasePermissionExceptionHandler {

    private static final ExceptionHandler INSTANCE = new GroupPermissionOperationExceptionHandler();

    @Override
    protected String getPermissionErrorMsg(
        String roles, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate role(s)%s operation [%s] under group [%s], reason [%s]",
          roles, operation, parent, reason);
    }
  }

  private static class RolePermissionOperationHandler extends BasePermissionExceptionHandler {

    private static final ExceptionHandler INSTANCE = new RolePermissionOperationHandler();

    @Override
    protected String getPermissionErrorMsg(
        String object, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate object(%s) operation [%s] under role [%s], reason [%s]",
          object, operation, parent, reason);
    }
  }

  private abstract static class BasePermissionExceptionHandler extends BaseExceptionHandler {

    protected abstract String getPermissionErrorMsg(
        String role, String operation, String parent, String reason);

    @Override
    public Response handle(OperationType op, String roles, String parent, Exception e) {
      String formatted = StringUtil.isBlank(roles) ? "" : " [" + roles + "]";
      String errorMsg = getPermissionErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, roles, parent, e);
      }
    }
  }

  private static class CredentialExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new CredentialExceptionHandler();

    private static String getCredentialErrorMsg(String parent, String reason) {
      return String.format(
          "Failed to get credentials under object [%s], reason [%s]", parent, reason);
    }

    @Override
    public Response handle(OperationType op, String credential, String parent, Exception e) {
      String errorMsg = getCredentialErrorMsg(parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, credential, parent, e);
      }
    }
  }

  private static class TagExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new TagExceptionHandler();

    private static String getTagErrorMsg(
        String tag, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate tag(s)%s operation [%s] under object [%s], reason [%s]",
          tag, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String tag, String parent, Exception e) {
      String formatted = StringUtil.isBlank(tag) ? "" : " [" + tag + "]";
      String errorMsg = getTagErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof TagAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof TagAlreadyAssociatedException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, tag, parent, e);
      }
    }
  }

  private static class PolicyExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new PolicyExceptionHandler();

    private static String getPolicyErrorMsg(
        String policy, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate policy(s)%s operation [%s] under object [%s], reason [%s]",
          policy, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String policy, String parent, Exception e) {
      String formatted = StringUtil.isBlank(policy) ? "" : " [" + policy + "]";
      String errorMsg = getPolicyErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof PolicyAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof PolicyAlreadyAssociatedException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, policy, parent, e);
      }
    }
  }

  private static class OwnerExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new OwnerExceptionHandler();

    private static String getOwnerErrorMsg(
        String name, String operation, String metalake, String reason) {
      return String.format(
          "Failed to execute %s owner operation [%s] under metalake [%s], reason [%s]",
          name, operation, metalake, reason);
    }

    @Override
    public Response handle(OperationType op, String name, String parent, Exception e) {
      String formatted = StringUtil.isBlank(name) ? "" : " [" + name + "]";
      String errorMsg = getOwnerErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, name, parent, e);
      }
    }
  }

  private static class ModelExceptionHandler extends BaseExceptionHandler {
    private static final ExceptionHandler INSTANCE = new ModelExceptionHandler();

    private static String getModelErrorMsg(
        String model, String operation, String schema, String reason) {
      return String.format(
          "Failed to operate model(s)%s operation [%s] under schema [%s], reason [%s]",
          model, operation, schema, reason);
    }

    @Override
    public Response handle(OperationType op, String model, String schema, Exception e) {
      String formatted = StringUtil.isBlank(model) ? "" : " [" + model + "]";
      String errorMsg = getModelErrorMsg(formatted, op.name(), schema, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof ModelAlreadyExistsException
          || e instanceof ModelVersionAliasesAlreadyExistException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else {
        return super.handle(op, model, schema, e);
      }
    }
  }

  private static class JobTemplateExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new JobTemplateExceptionHandler();

    private static String getJobTemplateErrorMsg(
        String jobTemplate, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate job template(s)%s operation [%s] under object [%s], reason [%s]",
          jobTemplate, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String jobTemplate, String parent, Exception e) {
      String formatted = StringUtil.isBlank(jobTemplate) ? "" : " [" + jobTemplate + "]";
      String errorMsg = getJobTemplateErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof JobTemplateAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof InUseException) {
        return Utils.inUse(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else {
        return super.handle(op, jobTemplate, parent, e);
      }
    }
  }

  private static class JobExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new JobExceptionHandler();

    private static String getJobErrorMsg(
        String jobTemplate, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate job(s)%s operation [%s] under object [%s], reason [%s]",
          jobTemplate, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String jobTemplate, String parent, Exception e) {
      String formatted = StringUtil.isBlank(jobTemplate) ? "" : " [" + jobTemplate + "]";
      String errorMsg = getJobErrorMsg(formatted, op.name(), parent, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof NotInUseException) {
        return Utils.notInUse(errorMsg, e);

      } else if (e instanceof ForbiddenException) {
        return Utils.forbidden(errorMsg, e);

      } else {
        return super.handle(op, jobTemplate, parent, e);
      }
    }
  }

  private static class StatisticExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new StatisticExceptionHandler();

    private static String getStatisticErrorMsg(
        String name, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate statistic(s)%s operation [%s] on parent [%s], reason [%s]",
          name, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String name, String object, Exception e) {
      String formatted = StringUtil.isBlank(name) ? "" : " [" + name + "]";
      String errorMsg = getStatisticErrorMsg(formatted, op.name(), object, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else {
        return super.handle(op, name, object, e);
      }
    }
  }

  private static class PartitionStatsExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new PartitionStatsExceptionHandler();

    private static String getPartitionStatsErrorMsg(
        String partition, String operation, String table, String reason) {
      return String.format(
          "Failed to operate partition stats%s operation [%s] of table [%s], reason [%s]",
          partition, operation, table, reason);
    }

    @Override
    public Response handle(OperationType op, String partition, String table, Exception e) {
      String formatted = StringUtil.isBlank(partition) ? "" : " [" + partition + "]";
      String errorMsg = getPartitionStatsErrorMsg(formatted, op.name(), table, getErrorMsg(e));
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof UnsupportedOperationException) {
        return Utils.unsupportedOperation(errorMsg, e);

      } else {
        return super.handle(op, partition, table, e);
      }
    }
  }

  @VisibleForTesting
  static class BaseExceptionHandler extends ExceptionHandler {

    private static final String EXCEPTION_KEYWORD = "Exception: ";

    private static String getBaseErrorMsg(
        String object, String operation, String parent, String reason) {
      return String.format(
          "Failed to operate object%s operation [%s]%s, reason [%s]",
          object, operation, parent, reason);
    }

    @Override
    public Response handle(OperationType op, String object, String parent, Exception e) {
      String formattedObject = StringUtil.isBlank(object) ? "" : " [" + object + "]";
      String formattedParent = StringUtil.isBlank(parent) ? "" : " under [" + parent + "]";

      String errorMsg =
          getBaseErrorMsg(formattedObject, op.name(), formattedParent, getErrorMsg(e));
      LOG.error(errorMsg, e);
      return Utils.internalError(errorMsg, e);
    }

    @VisibleForTesting
    static String getErrorMsg(Throwable throwable) {
      if (throwable == null || throwable.getMessage() == null) {
        return "";
      }

      String message = throwable.getMessage();
      int pos = message.lastIndexOf(EXCEPTION_KEYWORD);
      if (pos == -1) {
        return message;
      } else {
        return message.substring(pos + EXCEPTION_KEYWORD.length());
      }
    }
  }
}
