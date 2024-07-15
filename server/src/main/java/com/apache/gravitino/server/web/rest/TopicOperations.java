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
package com.apache.gravitino.server.web.rest;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.catalog.TopicDispatcher;
import com.apache.gravitino.dto.requests.TopicCreateRequest;
import com.apache.gravitino.dto.requests.TopicUpdateRequest;
import com.apache.gravitino.dto.requests.TopicUpdatesRequest;
import com.apache.gravitino.dto.responses.DropResponse;
import com.apache.gravitino.dto.responses.EntityListResponse;
import com.apache.gravitino.dto.responses.TopicResponse;
import com.apache.gravitino.dto.util.DTOConverters;
import com.apache.gravitino.lock.LockType;
import com.apache.gravitino.lock.TreeLockUtils;
import com.apache.gravitino.messaging.Topic;
import com.apache.gravitino.messaging.TopicChange;
import com.apache.gravitino.metrics.MetricNames;
import com.apache.gravitino.server.web.Utils;
import com.apache.gravitino.utils.NameIdentifierUtil;
import com.apache.gravitino.utils.NamespaceUtil;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
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

  private final TopicDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TopicOperations(TopicDispatcher dispatcher) {
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
      LOG.info("Received list topics request for schema: {}.{}.{}", metalake, catalog, schema);
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Listing topics under schema: {}.{}.{}", metalake, catalog, schema);
            Namespace topicNS = NamespaceUtil.ofTopic(metalake, catalog, schema);
            NameIdentifier[] topics =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog, schema),
                    LockType.READ,
                    () -> dispatcher.listTopics(topicNS));
            Response response = Utils.ok(new EntityListResponse(topics));
            LOG.info(
                "List {} topics under schema: {}.{}.{}", topics.length, metalake, catalog, schema);
            return response;
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
    LOG.info("Received create topic request: {}.{}.{}", metalake, catalog, schema);
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
                NameIdentifierUtil.ofTopic(metalake, catalog, schema, request.getName());

            Topic topic =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () ->
                        dispatcher.createTopic(
                            ident,
                            request.getComment(),
                            null /* dataLayout, always null because it's not supported yet.*/,
                            request.getProperties()));
            Response response = Utils.ok(new TopicResponse(DTOConverters.toDTO(topic)));
            LOG.info("Topic created: {}.{}.{}.{}", metalake, catalog, schema, topic.name());
            return response;
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
    LOG.info(
        "Received load topic request for topic: {}.{}.{}.{}", metalake, catalog, schema, topic);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Loading topic: {}.{}.{}.{}", metalake, catalog, schema, topic);
            NameIdentifier ident = NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic);
            Topic t = dispatcher.loadTopic(ident);
            Response response = Utils.ok(new TopicResponse(DTOConverters.toDTO(t)));
            LOG.info("Topic loaded: {}.{}.{}.{}", metalake, catalog, schema, topic);
            return response;
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
    LOG.info("Received alter topic request: {}.{}.{}.{}", metalake, catalog, schema, topic);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Altering topic: {}.{}.{}.{}", metalake, catalog, schema, topic);
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic);
            TopicChange[] changes =
                request.getUpdates().stream()
                    .map(TopicUpdateRequest::topicChange)
                    .toArray(TopicChange[]::new);

            Topic t =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.alterTopic(ident, changes));
            Response response = Utils.ok(new TopicResponse(DTOConverters.toDTO(t)));
            LOG.info("Topic altered: {}.{}.{}.{}", metalake, catalog, schema, t.name());
            return response;
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
    LOG.info("Received drop topic request: {}.{}.{}.{}", metalake, catalog, schema, topic);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            LOG.info("Dropping topic under schema: {}.{}.{}", metalake, catalog, schema);
            NameIdentifier ident = NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.dropTopic(ident));

            if (!dropped) {
              LOG.warn("Failed to drop topic {} under schema {}", topic, schema);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Topic dropped: {}.{}.{}.{}", metalake, catalog, schema, topic);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTopicException(OperationType.DROP, topic, schema, e);
    }
  }
}
