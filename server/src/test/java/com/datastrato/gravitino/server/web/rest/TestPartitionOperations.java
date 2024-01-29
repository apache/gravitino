/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.server.web.rest.TestTableOperations.mockColumn;
import static com.datastrato.gravitino.server.web.rest.TestTableOperations.mockTable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.PartitionNameListResponse;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionOperations extends JerseyTest {

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
                return new Partition[0];
              }

              @Override
              public Partition getPartition(String partitionName) throws NoSuchPartitionException {
                return null;
              }

              @Override
              public Partition addPartition(Partition partition)
                  throws PartitionAlreadyExistsException {
                return null;
              }

              @Override
              public boolean dropPartition(String partitionName) {
                return false;
              }
            });
    return mockedTable;
  }

  @Test
  public void testListPartitionNames() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    String partitionName1 = "p1";
    String partitionName2 = "p2";
    Table mockedTable =
        mockPartitionedTable(
            table,
            columns,
            "mock comment",
            ImmutableMap.of("k1", "v1"),
            new Transform[] {Transforms.identity("col1")},
            new String[] {partitionName1, partitionName2});
    when(dispatcher.loadTable(any())).thenReturn(mockedTable);

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
    Assertions.assertEquals(partitionName1, names[0]);
    Assertions.assertEquals(partitionName2, names[1]);

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
}
