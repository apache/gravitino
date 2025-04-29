/*
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.gravitino.lineage.source;

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
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.SneakyThrows;
import org.apache.gravitino.lineage.LineageDispatcher;
import org.apache.gravitino.lineage.source.rest.LineageOperations;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.api.Factory;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

// `createRunEvent` method is referred from
// client/java/src/test/java/io/openlineage/client/OpenLineageTest.java
public class TestLineageOperations extends JerseyTest {

  private ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

  private static class MockServletRequestFactory
      implements Factory<HttpServletRequest>, Supplier<HttpServletRequest> {

    @Override
    public HttpServletRequest provide() {
      return get();
    }

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRemoteUser()).thenReturn(null);
      return request;
    }

    @Override
    public void dispose(HttpServletRequest instance) {}
  }

  private LineageDispatcher lineageDispatcher = Mockito.mock(LineageDispatcher.class);

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
    Mockito.when(lineageDispatcher.dispatchLineageEvent(ArgumentMatchers.any())).thenReturn(true);
    Response resp =
        target("/lineage")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(runEvent, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Status.CREATED.getStatusCode(), resp.getStatus());
  }

  @SneakyThrows
  @Test
  public void testUpdateLineageFailed() {
    RunEvent runEvent = createRunEvent();
    Mockito.when(lineageDispatcher.dispatchLineageEvent(ArgumentMatchers.any())).thenReturn(false);
    Response resp =
        target("/lineage")
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
