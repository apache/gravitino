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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacets;
import java.io.IOException;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.SneakyThrows;
import org.apache.gravitino.lineage.LineageDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.lineage.rest.LineageOperations;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOpenLineageOperations extends JerseyTest {

  private ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private LineageDispatcher lineageDispatcher = mock(LineageDispatcher.class);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(LineageOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(lineageDispatcher).to(LineageDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @SneakyThrows
  @Test
  public void testUpdateLineageSucc() {
    RunEvent runEvent = createRunEvent();
    when(lineageDispatcher.dispatchLineageEvent(any())).thenReturn(true);
    Response resp =
        target("/v1/lineage")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(runEvent, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Status.CREATED.getStatusCode(), resp.getStatus());
  }

  @SneakyThrows
  @Test
  public void testUpdateLineageFailed() {
    RunEvent runEvent = createRunEvent();
    when(lineageDispatcher.dispatchLineageEvent(any())).thenReturn(false);
    Response resp =
        target("/v1/lineage")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(runEvent, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Status.TOO_MANY_REQUESTS.getStatusCode(), resp.getStatus());
  }

  private RunEvent createRunEvent() {
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);
    UUID runId = UUID.randomUUID();
    RunFacets runFacets =
        ol.newRunFacetsBuilder().nominalTime(ol.newNominalTimeRunFacet(now, now)).build();
    Run run = ol.newRun(runId, runFacets);
    String name = "jobName";
    String namespace = "namespace";
    JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    Job job = ol.newJob(namespace, name, jobFacets);
    List<InputDataset> inputs = Arrays.asList(ol.newInputDataset("ins", "input", null, null));
    List<OutputDataset> outputs = Arrays.asList(ol.newOutputDataset("ons", "output", null, null));
    RunEvent runStateUpdate =
        ol.newRunEvent(now, OpenLineage.RunEvent.EventType.START, run, job, inputs, outputs);
    return runStateUpdate;
  }
}
