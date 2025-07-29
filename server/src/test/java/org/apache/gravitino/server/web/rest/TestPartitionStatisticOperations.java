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

import static javax.ws.rs.client.Entity.entity;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.requests.PartitionStatsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatsUpdateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.PartitionStatsListResponse;
import org.apache.gravitino.dto.responses.PartitionStatsUpdateResponse;
import org.apache.gravitino.dto.stats.StatisticDTO;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.stats.PartitionStatisticManager;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPartitionStatisticOperations extends JerseyTest {

  private PartitionStatisticManager manager = mock(PartitionStatisticManager.class);

  private final String metalake = "metalake1";

  private final String catalog = "catalog1";

  private final String schema = "schema1";

  private final String table = "table1";

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

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
    resourceConfig.register(PartitionStatisticOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(manager).to(PartitionStatisticManager.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListTableStatistics() {

    StatisticDTO stat1 =
        StatisticDTO.builder()
            .withName("test1")
            .withValue(Optional.of(StatisticValues.stringValue("test")))
            .withReserved(true)
            .withModifiable(false)
            .build();
    StatisticDTO stat2 =
        StatisticDTO.builder()
            .withName("test1")
            .withValue(Optional.of(StatisticValues.longValue(1L)))
            .withReserved(true)
            .withModifiable(false)
            .build();
    Map<String, List<Statistic>> statsMap = Maps.newHashMap();
    statsMap.put("partition1", Lists.newArrayList(stat1, stat2));
    when(manager.listPartitionStatistics(any(), any(), any(), any())).thenReturn(statsMap);
    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionStatsListResponse listResp = resp.readEntity(PartitionStatsListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    List<StatisticDTO> statisticDTOS = listResp.getStatistics().get("partition1");
    Assertions.assertEquals(2, statisticDTOS.size());
    Assertions.assertEquals(stat1.name(), statisticDTOS.get(0).name());
    Assertions.assertEquals(stat1.value().get(), statisticDTOS.get(0).value().get());
    Assertions.assertEquals(stat2.name(), statisticDTOS.get(1).name());
    Assertions.assertEquals(stat2.value().get(), statisticDTOS.get(1).value().get());

    // Test throw NoSuchMetadataObjectException
    doThrow(new NoSuchMetadataObjectException("mock error"))
        .when(manager)
        .listPartitionStatistics(any(), any(), any(), any());
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(
        NoSuchMetadataObjectException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .listPartitionStatistics(any(), any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testUpdateTableStatistics() {

    StatisticDTO stat1 =
        StatisticDTO.builder()
            .withName(Statistic.CUSTOM_PREFIX + "test1")
            .withValue(Optional.of(StatisticValues.stringValue("test")))
            .withReserved(true)
            .withModifiable(false)
            .build();
    StatisticDTO stat2 =
        StatisticDTO.builder()
            .withName(Statistic.CUSTOM_PREFIX + "test2")
            .withValue(Optional.of(StatisticValues.longValue(1L)))
            .withReserved(true)
            .withModifiable(false)
            .build();

    Map<String, StatisticValue<?>> statsMap = Maps.newHashMap();
    statsMap.put(stat1.name(), stat1.value().get());
    statsMap.put(stat2.name(), stat2.value().get());
    Map<String, Map<String, StatisticValue<?>>> partitionMap = Maps.newHashMap();
    partitionMap.put("partition1", statsMap);

    PartitionStatsUpdateRequest req = new PartitionStatsUpdateRequest(partitionMap);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionStatsUpdateResponse updateResp = resp.readEntity(PartitionStatsUpdateResponse.class);
    Assertions.assertEquals(0, updateResp.getCode());

    List<StatisticDTO> statisticDTOS = updateResp.getStatistics().get("partition1");
    Assertions.assertEquals(2, statisticDTOS.size());
    Assertions.assertEquals(stat1.name(), statisticDTOS.get(1).name());
    Assertions.assertEquals(stat1.value().get(), statisticDTOS.get(1).value().get());
    Assertions.assertEquals(stat2.name(), statisticDTOS.get(0).name());
    Assertions.assertEquals(stat2.value().get(), statisticDTOS.get(0).value().get());

    // Test throw NoSuchMetadataObjectException
    doThrow(new NoSuchMetadataObjectException("mock error"))
        .when(manager)
        .updatePartitionStatistics(any(), any(), any());
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(
        NoSuchMetadataObjectException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .updatePartitionStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());

    // Test throw IllegalStatisticNameException
    statsMap.put("test1", stat1.value().get());

    req = new PartitionStatsUpdateRequest(partitionMap);
    Response resp3 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testDropTableStatistics() {
    Map<String, List<String>> partitionStatistics = Maps.newHashMap();
    partitionStatistics.put("partition1", Lists.newArrayList("stat1", "stat2"));
    PartitionStatsDropRequest req = new PartitionStatsDropRequest(partitionStatistics);
    when(manager.dropPartitionStatistics(any(), any(), any())).thenReturn(true);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test throw NoSuchMetadataObjectException
    doThrow(new NoSuchMetadataObjectException("mock error"))
        .when(manager)
        .dropPartitionStatistics(any(), any(), any());
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(
        NoSuchMetadataObjectException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .dropPartitionStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/catalogs/"
                    + catalog
                    + "/schemas/"
                    + schema
                    + "/tables/"
                    + table
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }
}
