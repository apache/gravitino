/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static com.datastrato.gravitino.server.web.rest.TestTableOperations.mockColumn;
import static com.datastrato.gravitino.server.web.rest.TestTableOperations.mockTable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.datastrato.gravitino.dto.requests.AddPartitionsRequest;
import com.datastrato.gravitino.dto.requests.DropPartitionsRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.PartitionListResponse;
import com.datastrato.gravitino.dto.responses.PartitionNameListResponse;
import com.datastrato.gravitino.dto.responses.PartitionResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPartitionOperations extends JerseyTest {

  private static final String[] partitionNames = new String[] {"p1", "p2"};
  private static final String[] colName = new String[] {"col1"};

  private static final Partition partition1 =
      Partitions.identity(
          partitionNames[0],
          new String[][] {colName},
          new Literal[] {Literals.stringLiteral("v1")},
          Maps.newHashMap());
  private static final Partition partition2 =
      Partitions.identity(
          partitionNames[1],
          new String[][] {colName},
          new Literal[] {Literals.stringLiteral("v2")},
          Maps.newHashMap());
  private static final Map<String, Partition> partitions =
      ImmutableMap.of(partitionNames[0], partition1, partitionNames[1], partition2);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CatalogOperationDispatcher dispatcher = mock(CatalogOperationDispatcher.class);
  private final String metalake = "metalake1";
  private final String catalog = "catalog1";
  private final String schema = "schema1";
  private final String table = "table1";

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(PartitionOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(CatalogOperationDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  private String partitionPath(String metalake, String catalog, String schema, String table) {
    return String.format(
        "/metalakes/%s/catalogs/%s/schemas/%s/tables/%s/partitions/",
        metalake, catalog, schema, table);
  }

  private Table mockPartitionedTable() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    String comment = "mock comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    Transform[] transforms = new Transform[] {Transforms.identity("col1")};
    return mockPartitionedTable(table, columns, comment, properties, transforms, partitionNames);
  }

  @SuppressWarnings("FormatStringAnnotation")
  private Table mockPartitionedTable(
      String tableName,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] transforms,
      String[] partitionNames) {
    Table mockedTable = mockTable(tableName, columns, comment, properties, transforms);
    when(mockedTable.supportPartitions())
        .thenReturn(
            new SupportsPartitions() {
              @Override
              public String[] listPartitionNames() {
                return partitionNames;
              }

              @Override
              public Partition[] listPartitions() {
                return partitions.values().toArray(new Partition[0]);
              }

              @Override
              public Partition getPartition(String partitionName) throws NoSuchPartitionException {
                Partition partition = partitions.get(partitionName);
                if (partition == null) {
                  throw new NoSuchPartitionException(partitionName);
                }
                return partition;
              }

              @Override
              public Partition addPartition(Partition partition)
                  throws PartitionAlreadyExistsException {
                if (partitions.containsKey(partition.name())) {
                  throw new PartitionAlreadyExistsException(partition.name());
                } else {
                  return partition;
                }
              }

              @Override
              public boolean dropPartition(String partitionName, boolean ifExists) {
                if (partitions.containsKey(partitionName)) {
                  return true;
                } else {
                  if (ifExists) {
                    return true;
                  } else {
                    throw new NoSuchPartitionException(partitionName);
                  }
                }
              }

              @Override
              public boolean dropPartitions(List<String> partitionNames, boolean ifExists)
                  throws NoSuchPartitionException, UnsupportedOperationException {
                if (partitions.containsKey(partitionNames.get(0))) {
                  return true;
                } else {
                  if (ifExists) {
                    return true;
                  } else {
                    throw new NoSuchPartitionException(partitionNames.get(0));
                  }
                }
              }
            });
    when(dispatcher.loadTable(any())).thenReturn(mockedTable);
    return mockedTable;
  }

  @Test
  public void testListPartitionNames() {
    Table mockedTable = mockPartitionedTable();

    Response resp =
        target(partitionPath(metalake, catalog, schema, table))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionNameListResponse listResp = resp.readEntity(PartitionNameListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    String[] names = listResp.partitionNames();
    Assertions.assertEquals(2, names.length);
    Assertions.assertEquals(partitionNames[0], names[0]);
    Assertions.assertEquals(partitionNames[1], names[1]);

    // Test throws exception
    doThrow(new RuntimeException("test exception")).when(mockedTable).supportPartitions();
    Response resp2 =
        target(partitionPath(metalake, catalog, schema, table))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
    Assertions.assertTrue(errorResp2.getMessage().contains("test exception"));
  }

  @Test
  public void testListPartitions() {
    Table mockedTable = mockPartitionedTable();

    Response resp =
        target(partitionPath(metalake, catalog, schema, table))
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionListResponse listResp = resp.readEntity(PartitionListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    Partition[] partitions = listResp.getPartitions();
    Assertions.assertEquals(2, partitions.length);
    Assertions.assertEquals(DTOConverters.toDTO(partition1), partitions[0]);
    Assertions.assertEquals(DTOConverters.toDTO(partition2), partitions[1]);

    // Test throws exception
    doThrow(new RuntimeException("test exception")).when(mockedTable).supportPartitions();
    Response resp2 =
        target(partitionPath(metalake, catalog, schema, table))
            .queryParam("details", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
    Assertions.assertTrue(errorResp2.getMessage().contains("test exception"));
  }

  @Test
  public void testGetPartition() {
    mockPartitionedTable();

    Response resp =
        target(partitionPath(metalake, catalog, schema, table) + partitionNames[0])
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionResponse partitionResp = resp.readEntity(PartitionResponse.class);
    Assertions.assertEquals(0, partitionResp.getCode());

    Partition partition = partitionResp.getPartition();
    Assertions.assertEquals(DTOConverters.toDTO(partition1), partition);

    // Test throws exception
    Response resp2 =
        target(partitionPath(metalake, catalog, schema, table) + "p3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp2.getCode());
    Assertions.assertEquals(NoSuchPartitionException.class.getSimpleName(), errorResp2.getType());
    Assertions.assertTrue(errorResp2.getMessage().contains("p3"));
  }

  @Test
  public void testAddPartition() {
    mockPartitionedTable();

    Partition newPartition =
        Partitions.identity(
            "p3",
            new String[][] {colName},
            new Literal[] {Literals.stringLiteral("v3")},
            Maps.newHashMap());

    AddPartitionsRequest req =
        new AddPartitionsRequest(new PartitionDTO[] {DTOConverters.toDTO(newPartition)});
    Response resp =
        target(partitionPath(metalake, catalog, schema, table))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionListResponse partitionResp = resp.readEntity(PartitionListResponse.class);
    Assertions.assertEquals(0, partitionResp.getCode());

    Partition[] partition = partitionResp.getPartitions();
    Assertions.assertEquals(1, partition.length);
    Assertions.assertEquals(DTOConverters.toDTO(newPartition), partition[0]);

    // Test throws exception
    req = new AddPartitionsRequest(new PartitionDTO[] {DTOConverters.toDTO(partition1)});
    Response resp2 =
        target(partitionPath(metalake, catalog, schema, table))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        PartitionAlreadyExistsException.class.getSimpleName(), errorResp2.getType());
    Assertions.assertTrue(errorResp2.getMessage().contains(partition1.name()));
  }

  @Test
  public void testDropPartition() {
    mockPartitionedTable();

    // drop exist partition with ifExists=ture
    Response resp =
        target(partitionPath(metalake, catalog, schema, table) + "p1")
            .queryParam("purge", "false")
            .queryParam("ifExists", "true")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test throws exception, drop no-exist partition with ifExists=false
    Response resp1 =
        target(partitionPath(metalake, catalog, schema, table) + "p5")
            .queryParam("purge", "false")
            .queryParam("ifExists", "false")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchPartitionException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  public void testDropPartitions() {
    mockPartitionedTable();

    // drop partition, only one partition is supported
    String[] partitionNames = {"p1"};
    DropPartitionsRequest req = new DropPartitionsRequest(partitionNames);
    Response resp =
        target(partitionPath(metalake, catalog, schema, table) + "delete")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());
  }
}
