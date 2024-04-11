/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.TopicOperationDispatcher;
import com.datastrato.gravitino.dto.requests.TopicCreateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.TopicResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/topics")
public class TopicOperations {
  private static final Logger LOG = LoggerFactory.getLogger(TopicOperations.class);

  private final TopicOperationDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TopicOperations(TopicOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-topic." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  public Response listTopics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Listing topics under schema: {}.{}.{}", metalake, catalog, schema);
            Namespace topicNS = Namespace.ofTopic(metalake, catalog, schema);
            NameIdentifier[] topics =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog, schema),
                    LockType.READ,
                    () -> dispatcher.listTopics(topicNS));
            return Utils.ok(new EntityListResponse(topics));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-topic." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-topic", absolute = true)
  public Response createTopic(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      TopicCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info(
                "Creating topic under schema: {}.{}.{}.{}",
                metalake,
                catalog,
                schema,
                request.getName());
            request.validate();
            NameIdentifier ident =
                NameIdentifier.ofTopic(metalake, catalog, schema, request.getName());

            Topic topic =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () ->
                        dispatcher.createTopic(
                            ident,
                            request.getComment(),
                            null /* dataLayout, always null because it's not supported yet.*/,
                            request.getProperties()));
            return Utils.ok(new TopicResponse(DTOConverters.toDTO(topic)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTopicException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("/{topic}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-topic." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-topic", absolute = true)
  public Response loadTopic(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("topic") String topic) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Loading topic: {}.{}.{}.{}", metalake, catalog, schema, topic);
            NameIdentifier ident = NameIdentifier.ofTopic(metalake, catalog, schema, topic);
            Topic t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.READ, () -> dispatcher.loadTopic(ident));
            return Utils.ok(new TopicResponse(DTOConverters.toDTO(t)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTopicException(OperationType.LOAD, topic, schema, e);
    }
  }

  @PUT
  @Path("/{topic}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-topic." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-topic", absolute = true)
  public Response alterTopic(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("topic") String topic,
      TopicUpdatesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Altering topic: {}.{}.{}.{}", metalake, catalog, schema, topic);
            request.validate();
            NameIdentifier ident = NameIdentifier.ofTopic(metalake, catalog, schema, topic);
            TopicChange[] changes =
                request.getUpdates().stream()
                    .map(TopicUpdateRequest::topicChange)
                    .toArray(TopicChange[]::new);

            Topic t =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.alterTopic(ident, changes));
            return Utils.ok(new TopicResponse(DTOConverters.toDTO(t)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTopicException(OperationType.ALTER, topic, schema, e);
    }
  }

  @DELETE
  @Path("/{topic}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-topic." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-topic", absolute = true)
  public Response dropTopic(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("topic") String topic) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Dropping topic under schema: {}.{}.{}", metalake, catalog, schema);
            NameIdentifier ident = NameIdentifier.ofTopic(metalake, catalog, schema, topic);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.dropTopic(ident));

            if (!dropped) {
              LOG.warn("Failed to drop topic {} under schema {}", topic, schema);
            }

            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTopicException(OperationType.DROP, topic, schema, e);
    }
  }
}
