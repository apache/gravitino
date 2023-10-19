/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.PartitionUtils;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.json.JsonUtils;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Path("/")
public class GravitinoRestApi {

  private static String testMetalakeName = "test";
  private static String testCatalogName = "memory";
  private static Map<String, SchemaDTO> testSchemas = new HashMap<>();
  private static Map<String, TableDTO> testTables = new HashMap<>();

  @GET
  @Path("/metalakes/{name}")
  public Response loadMetalake(@PathParam("name") String metalakeName) throws Exception {
    if (!testMetalakeName.equals(metalakeName)) {
      throw new NoSuchMetalakeException("Metalake does not exist");
    }
    MetalakeDTO metalake =
        new MetalakeDTO.Builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(new AuditDTO.Builder().build())
            .build();
    return ok(new MetalakeResponse(metalake));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs")
  public Response listCatalogs(@PathParam("metalake") String metalake) throws Exception {
    NameIdentifier[] idents = {NameIdentifier.ofCatalog(metalake, testCatalogName)};
    return ok(new EntityListResponse(idents));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs/{catalog}")
  public Response loadCatalog(
      @PathParam("metalake") String metalakeName, @PathParam("catalog") String catalogName)
      throws Exception {
    if (!metalakeName.equals(testMetalakeName) || !catalogName.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }
    CatalogDTO catalog =
        new CatalogDTO.Builder()
            .withName(testCatalogName)
            .withProvider("memory")
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(Collections.emptyMap())
            .withAudit(new AuditDTO.Builder().build())
            .build();

    return ok(new CatalogResponse(catalog));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas")
  public Response listSchemas(
      @PathParam("metalake") String metalake, @PathParam("catalog") String catalog)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    NameIdentifier[] schemas =
        testSchemas.entrySet().stream()
            .map((e) -> NameIdentifier.ofSchema(testMetalakeName, testCatalogName, e.getKey()))
            .toArray(NameIdentifier[]::new);
    return ok(new EntityListResponse(schemas));
  }

  @POST
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas")
  public Response createSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      String requestStr)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    SchemaDTO originSchema = JsonUtils.objectMapper().readValue(requestStr, SchemaDTO.class);

    if (testSchemas.containsKey(originSchema.name())) {
      throw new SchemaAlreadyExistsException("Schema already exists");
    }

    SchemaDTO schemaDTO =
        new SchemaDTO.Builder()
            .withName(originSchema.name())
            .withComment(originSchema.comment())
            .withProperties(originSchema.properties())
            .withAudit(new AuditDTO.Builder().build())
            .build();
    testSchemas.put(schemaDTO.name(), schemaDTO);

    return ok(new SchemaResponse(schemaDTO));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}")
  public Response loadSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    SchemaDTO schemaDTO = testSchemas.get(schema);
    if (schemaDTO == null) {
      throw new NoSuchSchemaException("Schema does not exist");
    }

    return ok(new SchemaResponse(schemaDTO));
  }

  @DELETE
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}")
  public Response dropSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }
    if (!testTables.isEmpty() && cascade == false) {
      throw new NonEmptySchemaException("No");
    }
    testSchemas.remove(schema);
    return ok(new DropResponse(true));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables")
  public Response listTables(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema)
      throws Exception {

    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    if (!testSchemas.containsKey(schema)) {
      throw new NoSuchSchemaException("Schema does not exist");
    }

    NameIdentifier[] tables =
        testTables.entrySet().stream()
            .map(
                (e) ->
                    NameIdentifier.ofTable(testMetalakeName, testCatalogName, schema, e.getKey()))
            .toArray(NameIdentifier[]::new);
    return ok(new EntityListResponse(tables));
  }

  @POST
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables")
  public Response createTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      String requestStr)
      throws Exception {

    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    if (!testSchemas.containsKey(schema)) {
      throw new NoSuchSchemaException("Schema does not exist");
    }

    TableDTO originTable = JsonUtils.objectMapper().readValue(requestStr, TableDTO.class);

    if (testTables.containsKey(originTable.name())) {
      throw new TableAlreadyExistsException("Table already exists");
    }

    TableDTO table =
        TableDTO.builder()
            .withName(originTable.name())
            .withComment(originTable.comment())
            .withColumns((ColumnDTO[]) originTable.columns())
            .withProperties(originTable.properties())
            .withPartitions(PartitionUtils.toPartitions(originTable.partitioning()))
            .withDistribution(DTOConverters.toDTO(originTable.distribution()))
            .withSortOrders(DTOConverters.toDTOs(originTable.sortOrder()))
            .withAudit(new AuditDTO.Builder().build())
            .build();

    testTables.put(table.name(), table);
    return ok(new TableResponse(table));
  }

  @GET
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public Response loadTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    if (!testSchemas.containsKey(schema)) {
      throw new NoSuchSchemaException("Schema does not exist");
    }

    TableDTO tableDTO = testTables.get(table);
    if (tableDTO == null) {
      NoSuchTableException tableDoesNotExist = new NoSuchTableException("Table does not exist");
      return notFound(
          NoSuchTableException.class.getSimpleName(),
          tableDoesNotExist.getMessage(),
          tableDoesNotExist);
    }
    return ok(new TableResponse(tableDTO));
  }

  @DELETE
  @Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public Response dropTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @QueryParam("purge") @DefaultValue("false") boolean purge)
      throws Exception {
    if (!metalake.equals(testMetalakeName) || !catalog.equals(testCatalogName)) {
      throw new NoSuchCatalogException("Catalog does not exist");
    }

    if (!testTables.containsKey(schema)) {
      throw new NoSuchSchemaException("Schema does not exist");
    }

    TableDTO tableDTO = testTables.get(table);
    boolean dropped = false;
    if (testTables.containsKey(table)) {
      dropped = true;
      testTables.remove(table);
    }

    return ok(new DropResponse(dropped));
  }

  public static <T> Response ok(T t) throws Exception {
    String str = JsonUtils.objectMapper().writeValueAsString(t);
    return Response.status(Response.Status.OK).entity(str).type(MediaType.APPLICATION_JSON).build();
  }

  public static <T> Response notFound(String type, String msg, Throwable t) throws Exception {
    ErrorResponse errorResponse = ErrorResponse.notFound(type, msg, t);
    String str = JsonUtils.objectMapper().writeValueAsString(errorResponse);
    return Response.status(Response.Status.NOT_FOUND)
        .entity(str)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
