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
import java.time.Instant;
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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.dto.requests.PartitionStatisticsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatisticsUpdateRequest;
import org.apache.gravitino.dto.requests.StatisticsDropRequest;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.PartitionStatisticsListResponse;
import org.apache.gravitino.dto.responses.StatisticListResponse;
import org.apache.gravitino.dto.stats.PartitionStatisticsDropDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsUpdateDTO;
import org.apache.gravitino.dto.stats.StatisticDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestStatisticOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private static TableDispatcher tableDispatcher = mock(TableDispatcher.class);
  private StatisticManager manager = mock(StatisticManager.class);

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
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tableDispatcher, true);
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
    resourceConfig.register(StatisticOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(manager).to(StatisticManager.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListTableStatistics() {

    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreateTime(Instant.now())
            .withCreator("test")
            .withLastModifiedTime(Instant.now())
            .withLastModifier("test")
            .build();

    Statistic stat1 =
        new StatisticManager.CustomStatistic(
            "test", StatisticValues.stringValue("test"), auditInfo);

    Statistic stat2 =
        new StatisticManager.CustomStatistic("test2", StatisticValues.longValue(1L), auditInfo);

    when(manager.listStatistics(any(), any())).thenReturn(Lists.newArrayList(stat1, stat2));
    when(tableDispatcher.tableExists(any())).thenReturn(true);

    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);
    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    StatisticListResponse listResp = resp.readEntity(StatisticListResponse.class);
    listResp.validate();
    Assertions.assertEquals(0, listResp.getCode());

    StatisticDTO[] statisticDTOS = listResp.getStatistics();
    Assertions.assertEquals(2, statisticDTOS.length);
    Assertions.assertEquals(stat1.name(), statisticDTOS[0].name());
    Assertions.assertEquals(DTOConverters.toDTO(auditInfo), statisticDTOS[0].auditInfo());
    Assertions.assertEquals(stat1.value().get(), statisticDTOS[0].value().get());
    Assertions.assertEquals(stat2.name(), statisticDTOS[1].name());
    Assertions.assertEquals(stat2.value().get(), statisticDTOS[1].value().get());
    Assertions.assertEquals(DTOConverters.toDTO(auditInfo), statisticDTOS[1].auditInfo());

    // Test throw NoSuchMetadataObjectException
    when(tableDispatcher.tableExists(any())).thenReturn(false);

    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
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
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    doThrow(new RuntimeException("mock error")).when(manager).listStatistics(any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
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
    Map<String, StatisticValue<?>> statsMap = Maps.newHashMap();
    statsMap.put(Statistic.CUSTOM_PREFIX + "test1", StatisticValues.stringValue("test"));
    statsMap.put(Statistic.CUSTOM_PREFIX + "test2", StatisticValues.longValue(1L));

    StatisticsUpdateRequest req = new StatisticsUpdateRequest(statsMap);
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);

    when(tableDispatcher.tableExists(any())).thenReturn(true);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    BaseResponse updateResp = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, updateResp.getCode());

    // Test throw NoSuchMetadataObjectException
    when(tableDispatcher.tableExists(any())).thenReturn(false);
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
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
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    doThrow(new RuntimeException("mock error")).when(manager).updateStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
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
    statsMap.put("test1", StatisticValues.longValue(1L));

    req = new StatisticsUpdateRequest(statsMap);
    Response resp3 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(
        IllegalStatisticNameException.class.getSimpleName(), errorResp3.getType());

    // Test throw UnmodifiableStatisticException
    doThrow(new UnmodifiableStatisticException("mock error"))
        .when(manager)
        .updateStatistics(any(), any(), any());
    statsMap.clear();
    statsMap.put(Statistic.CUSTOM_PREFIX + "test1", StatisticValues.stringValue("test"));
    req = new StatisticsUpdateRequest(statsMap);

    Response resp4 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), resp4.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp4.getMediaType());

    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResp4.getCode());
    Assertions.assertEquals(
        UnmodifiableStatisticException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testDropTableStatistics() {
    StatisticsDropRequest req = new StatisticsDropRequest(new String[] {"test1", "test2"});
    when(manager.dropStatistics(any(), any(), any())).thenReturn(true);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test throw NoSuchMetadataObjectException
    when(tableDispatcher.tableExists(any())).thenReturn(false);
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
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
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    doThrow(new RuntimeException("mock error")).when(manager).dropStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());

    // Test throw UnmodifiableStatisticException
    doThrow(new UnmodifiableStatisticException("mock error"))
        .when(manager)
        .dropStatistics(any(), any(), any());
    Response resp3 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResp3.getCode());
    Assertions.assertEquals(
        UnmodifiableStatisticException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testListPartitionStatistics() {
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreateTime(Instant.now())
            .withCreator("test")
            .withLastModifiedTime(Instant.now())
            .withLastModifier("test")
            .build();

    StatisticDTO stat1 =
        StatisticDTO.builder()
            .withName("test1")
            .withValue(Optional.of(StatisticValues.stringValue("test")))
            .withReserved(true)
            .withModifiable(false)
            .withAudit(DTOConverters.toDTO(auditInfo))
            .build();
    StatisticDTO stat2 =
        StatisticDTO.builder()
            .withName("test1")
            .withValue(Optional.of(StatisticValues.longValue(1L)))
            .withReserved(true)
            .withModifiable(false)
            .withAudit(DTOConverters.toDTO(auditInfo))
            .build();
    PartitionStatistics partitionStatistics =
        new StatisticManager.CustomPartitionStatistic("partition1", new Statistic[] {stat1, stat2});
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);

    when(manager.listPartitionStatistics(any(), any(), any()))
        .thenReturn(Lists.newArrayList(partitionStatistics));
    when(tableDispatcher.tableExists(any())).thenReturn(true);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .queryParam("from", "p0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PartitionStatisticsListResponse listResp =
        resp.readEntity(PartitionStatisticsListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    Statistic[] statisticDTOS = listResp.getPartitionStatistics()[0].statistics();
    Assertions.assertEquals(2, statisticDTOS.length);
    Assertions.assertEquals(stat1.name(), statisticDTOS[0].name());
    Assertions.assertEquals(stat1.value().get(), statisticDTOS[0].value().get());
    Assertions.assertEquals(stat2.name(), statisticDTOS[1].name());
    Assertions.assertEquals(stat2.value().get(), statisticDTOS[1].value().get());

    // Test throw NoSuchMetadataObjectException
    when(tableDispatcher.tableExists(any())).thenReturn(false);
    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .queryParam("from", "p0")
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
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .listPartitionStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .queryParam("from", "p0")
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
  public void testUpdatePartitionStatistics() {
    Map<String, StatisticValue<?>> statsMap = Maps.newHashMap();
    statsMap.put(Statistic.CUSTOM_PREFIX + "test1", StatisticValues.stringValue("test"));
    statsMap.put(Statistic.CUSTOM_PREFIX + "test2", StatisticValues.longValue(1L));
    List<PartitionStatisticsUpdateDTO> partitionStatsList = Lists.newArrayList();
    partitionStatsList.add(PartitionStatisticsUpdateDTO.of("partition1", statsMap));
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);

    PartitionStatisticsUpdateRequest req = new PartitionStatisticsUpdateRequest(partitionStatsList);

    when(tableDispatcher.tableExists(any())).thenReturn(true);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    BaseResponse updateResp = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, updateResp.getCode());

    // Test throw NoSuchMetadataObjectException
    when(tableDispatcher.tableExists(any())).thenReturn(false);

    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
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

    when(tableDispatcher.tableExists(any())).thenReturn(true);

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .updatePartitionStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
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
    statsMap.put("test1", StatisticValues.longValue(1L));

    req = new PartitionStatisticsUpdateRequest(partitionStatsList);
    Response resp3 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(
        IllegalStatisticNameException.class.getSimpleName(), errorResp3.getType());

    // Test throw UnmodifiableStatisticException
    statsMap.clear();
    statsMap.put(Statistic.CUSTOM_PREFIX + "test1", StatisticValues.longValue(1L));
    doThrow(new UnmodifiableStatisticException("mock error"))
        .when(manager)
        .updatePartitionStatistics(any(), any(), any());

    req = new PartitionStatisticsUpdateRequest(partitionStatsList);
    Response resp4 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), resp4.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp4.getMediaType());

    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResp4.getCode());
    Assertions.assertEquals(
        UnmodifiableStatisticException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testDropPartitionStatistics() {
    List<PartitionStatisticsDropDTO> partitionStatistics = Lists.newArrayList();
    partitionStatistics.add(
        PartitionStatisticsDropDTO.of("partition1", Lists.newArrayList("stat1", "stat2")));
    PartitionStatisticsDropRequest req = new PartitionStatisticsDropRequest(partitionStatistics);
    when(manager.dropPartitionStatistics(any(), any(), any())).thenReturn(true);
    when(tableDispatcher.tableExists(any())).thenReturn(true);

    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("%s.%s.%s", catalog, schema, table), MetadataObject.Type.TABLE);

    Response resp =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test throw NoSuchMetadataObjectExcep
    when(tableDispatcher.tableExists(any())).thenReturn(false);

    Response resp1 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
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
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .dropPartitionStatistics(any(), any(), any());
    Response resp2 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
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

    // Test throw UnmodifiableStatisticException
    doThrow(new UnmodifiableStatisticException("mock error"))
        .when(manager)
        .dropPartitionStatistics(any(), any(), any());
    Response resp3 =
        target(
                "/metalakes/"
                    + metalake
                    + "/objects/"
                    + tableObject.type()
                    + "/"
                    + tableObject.fullName()
                    + "/statistics/partitions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResp3.getCode());
    Assertions.assertEquals(
        UnmodifiableStatisticException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testGetBoundType() {
    Assertions.assertEquals(
        PartitionRange.BoundType.CLOSED, StatisticOperations.getFromBoundType(true));
    Assertions.assertEquals(
        PartitionRange.BoundType.OPEN, StatisticOperations.getFromBoundType(false));
  }
}
