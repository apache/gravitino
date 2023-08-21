/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.NotFoundException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.server.web.Utils;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandlers.class);

  private ExceptionHandlers() {}

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

  private static class TableExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new TableExceptionHandler();

    private static final String TABLE_MSG_TEMPLATE =
        "Failed to operate table(s)%s operation [%s] under schema [%s], reason [%s]";

    @Override
    public Response handle(OperationType op, String table, String schema, Exception e) {
      String formatted = StringUtil.isBlank(table) ? "" : " [" + table + "]";
      String errorMsg =
          String.format(
              TABLE_MSG_TEMPLATE, formatted, op.name(), schema, e.getClass().getSimpleName());
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof TableAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else {
        return super.handle(op, table, schema, e);
      }
    }
  }

  private static class SchemaExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new SchemaExceptionHandler();

    private static final String SCHEMA_MSG_TEMPLATE =
        "Failed to operate schema(s)%s operation [%s] under catalog [%s], reason [%s]";

    @Override
    public Response handle(OperationType op, String schema, String catalog, Exception e) {
      String formatted = StringUtil.isBlank(schema) ? "" : " [" + schema + "]";
      String errorMsg =
          String.format(
              SCHEMA_MSG_TEMPLATE, formatted, op.name(), catalog, e.getClass().getSimpleName());
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof SchemaAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NonEmptySchemaException) {
        return Utils.nonEmpty(errorMsg, e);

      } else {
        return super.handle(op, schema, catalog, e);
      }
    }
  }

  private static class CatalogExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new CatalogExceptionHandler();

    private static final String CATALOG_MSG_TEMPLATE =
        "Failed to operate catalog(s)%s operation [%s] under metalake [%s], reason [%s]";

    @Override
    public Response handle(OperationType op, String catalog, String metalake, Exception e) {
      String formatted = StringUtil.isBlank(catalog) ? "" : " [" + catalog + "]";
      String errorMsg =
          String.format(
              CATALOG_MSG_TEMPLATE, formatted, op.name(), metalake, e.getClass().getSimpleName());
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof NotFoundException) {
        return Utils.notFound(errorMsg, e);

      } else if (e instanceof CatalogAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else {
        return super.handle(op, catalog, metalake, e);
      }
    }
  }

  private static class MetalakeExceptionHandler extends BaseExceptionHandler {

    private static final ExceptionHandler INSTANCE = new MetalakeExceptionHandler();

    private static final String METALAKE_MSG_TEMPLATE =
        "Failed to operate metalake(s)%s operation [%s], reason [%s]";

    @Override
    public Response handle(OperationType op, String metalake, String parent, Exception e) {
      String formatted = StringUtil.isBlank(metalake) ? "" : " [" + metalake + "]";
      String errorMsg =
          String.format(METALAKE_MSG_TEMPLATE, formatted, op.name(), e.getClass().getSimpleName());
      LOG.warn(errorMsg, e);

      if (e instanceof IllegalArgumentException) {
        return Utils.illegalArguments(errorMsg, e);

      } else if (e instanceof MetalakeAlreadyExistsException) {
        return Utils.alreadyExists(errorMsg, e);

      } else if (e instanceof NoSuchMetalakeException) {
        return Utils.notFound(errorMsg, e);

      } else {
        return super.handle(op, metalake, parent, e);
      }
    }
  }

  private static class BaseExceptionHandler extends ExceptionHandler {

    private static final ExceptionHandler INSTANCE = new BaseExceptionHandler();

    private static final String BASE_MSG_TEMPLATE =
        "Failed to operate object%s operation [%s]%s, reason [%s]";

    @Override
    public Response handle(OperationType op, String object, String parent, Exception e) {
      String formattedObject = StringUtil.isBlank(object) ? "" : " [" + object + "]";
      String formattedParent = StringUtil.isBlank(parent) ? "" : " under [" + parent + "]";

      String errorMsg =
          String.format(
              BASE_MSG_TEMPLATE,
              formattedObject,
              op.name(),
              formattedParent,
              e.getClass().getSimpleName());
      LOG.error(errorMsg, e);
      return Utils.internalError(errorMsg, e);
    }
  }
}
