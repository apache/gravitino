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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.catalog.PartitionOperationDispatcher;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.requests.AddPartitionsRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.PartitionListResponse;
import org.apache.gravitino.dto.responses.PartitionNameListResponse;
import org.apache.gravitino.dto.responses.PartitionResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rest.RESTUtils;
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
  private static final Partition[] partitions = new Partition[] {partition1, partition2};

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private PartitionOperationDispatcher dispatcher = mock(PartitionOperationDispatcher.class);
  private final String metalake = "metalake1";
  private final String catalog = "catalog1";
  private final String schema = "schema1";
  private final String table = "table1";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
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
            bind(dispatcher).to(PartitionDispatcher.class).ranked(2);
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

  @Test
  public void testListPartitionNames() {
    when(dispatcher.listPartitionNames(any())).thenReturn(partitionNames);

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
    doThrow(new RuntimeException("test exception")).when(dispatcher).listPartitionNames(any());
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
    when(dispatcher.listPartitions(any())).thenReturn(partitions);

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
    doThrow(new RuntimeException("test exception")).when(dispatcher).listPartitions(any());
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
    when(dispatcher.getPartition(any(), any())).thenReturn(partition1);

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
    doThrow(new NoSuchPartitionException("p3")).when(dispatcher).getPartition(any(), any());

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
    when(dispatcher.addPartition(any(), any())).thenReturn(partition1);

    AddPartitionsRequest req =
        new AddPartitionsRequest(new PartitionDTO[] {DTOConverters.toDTO(partition1)});
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
    Assertions.assertEquals(DTOConverters.toDTO(partition1), partition[0]);

    // Test throws exception
    doThrow(new PartitionAlreadyExistsException("mock error"))
        .when(dispatcher)
        .addPartition(any(), any());

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
    Assertions.assertTrue(errorResp2.getMessage().contains("mock error"));
  }

  @Test
  public void testDropPartition() {
    when(dispatcher.dropPartition(any(), any())).thenReturn(true);
    Response resp =
        target(partitionPath(metalake, catalog, schema, table) + "p1")
            .queryParam("purge", "false")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test drop no-exist partition and return false
    when(dispatcher.dropPartition(any(), any())).thenReturn(false);
    Response resp1 =
        target(partitionPath(metalake, catalog, schema, table) + "p5")
            .queryParam("purge", "false")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    doThrow(new RuntimeException("test exception")).when(dispatcher).dropPartition(any(), any());
    DropResponse noExistDropResponse = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, noExistDropResponse.getCode());
    Assertions.assertFalse(noExistDropResponse.dropped());
  }
}
