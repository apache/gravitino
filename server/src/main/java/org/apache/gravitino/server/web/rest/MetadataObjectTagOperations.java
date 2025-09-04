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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/tags")
public class MetadataObjectTagOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectTagOperations.class);

  private final TagDispatcher tagDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetadataObjectTagOperations(TagDispatcher tagDispatcher) {
    this.tagDispatcher = tagDispatcher;
  }

  // TagOperations will reuse this class to be compatible with legacy interfaces.
  void setHttpRequest(HttpServletRequest httpRequest) {
    this.httpRequest = httpRequest;
  }

  @GET
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-object-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-object-tag", absolute = true)
  public Response getTagForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @PathParam("tag") String tagName) {
    LOG.info(
        "Received get tag {} request for object type: {}, full name: {} under metalake: {}",
        tagName,
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            Optional<Tag> tag = getTagForObject(metalake, object, tagName);
            Optional<TagDTO> tagDTO = tag.map(t -> DTOConverters.toDTO(t, Optional.of(false)));

            MetadataObject parentObject = MetadataObjects.parent(object);
            while (!tag.isPresent() && parentObject != null) {
              tag = getTagForObject(metalake, parentObject, tagName);
              tagDTO = tag.map(t -> DTOConverters.toDTO(t, Optional.of(true)));
              parentObject = MetadataObjects.parent(parentObject);
            }

            if (!tagDTO.isPresent()) {
              LOG.warn(
                  "Tag {} not found for object type: {}, full name: {} under metalake: {}",
                  tagName,
                  type,
                  fullName,
                  metalake);
              return Utils.notFound(
                  NoSuchTagException.class.getSimpleName(),
                  "Tag not found: "
                      + tagName
                      + " for object type: "
                      + type
                      + ", full name: "
                      + fullName
                      + " under metalake: "
                      + metalake);
            } else {
              LOG.info(
                  "Get tag: {} for object type: {}, full name: {} under metalake: {}",
                  tagName,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new TagResponse(tagDTO.get()));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.GET, tagName, fullName, e);
    }
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-tags", absolute = true)
  public Response listTagsForMetadataObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list tag {} request for object type: {}, full name: {} under metalake: {}",
        verbose ? "infos" : "names",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));

            Set<TagDTO> tags = Sets.newHashSet();
            Tag[] nonInheritedTags = tagDispatcher.listTagsInfoForMetadataObject(metalake, object);
            if (ArrayUtils.isNotEmpty(nonInheritedTags)) {
              Collections.addAll(
                  tags,
                  Arrays.stream(nonInheritedTags)
                      .map(t -> DTOConverters.toDTO(t, Optional.of(false)))
                      .toArray(TagDTO[]::new));
            }

            MetadataObject parentObject = MetadataObjects.parent(object);
            while (parentObject != null) {
              Tag[] inheritedTags =
                  tagDispatcher.listTagsInfoForMetadataObject(metalake, parentObject);
              if (ArrayUtils.isNotEmpty(inheritedTags)) {
                Collections.addAll(
                    tags,
                    Arrays.stream(inheritedTags)
                        .map(t -> DTOConverters.toDTO(t, Optional.of(true)))
                        .toArray(TagDTO[]::new));
              }
              parentObject = MetadataObjects.parent(parentObject);
            }

            if (verbose) {
              LOG.info(
                  "List {} tags info for object type: {}, full name: {} under metalake: {}",
                  tags.size(),
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new TagListResponse(tags.toArray(new TagDTO[0])));

            } else {
              // We have used Set to avoid duplicate tag names
              String[] tagNames = tags.stream().map(TagDTO::name).toArray(String[]::new);

              LOG.info(
                  "List {} tags for object type: {}, full name: {} under metalake: {}",
                  tagNames.length,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new NameListResponse(tagNames));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", fullName, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "associate-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-tags", absolute = true)
  public Response associateTagsForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      TagsAssociateRequest request) {
    LOG.info(
        "Received associate tags request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            String[] tagNames =
                tagDispatcher.associateTagsForMetadataObject(
                    metalake, object, request.getTagsToAdd(), request.getTagsToRemove());
            tagNames = tagNames == null ? new String[0] : tagNames;

            LOG.info(
                "Associated tags: {} for object type: {}, full name: {} under metalake: {}",
                Arrays.toString(tagNames),
                type,
                fullName,
                metalake);
            return Utils.ok(new NameListResponse(tagNames));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.ASSOCIATE, "", fullName, e);
    }
  }

  private Optional<Tag> getTagForObject(String metalake, MetadataObject object, String tagName) {
    try {
      return Optional.ofNullable(tagDispatcher.getTagForMetadataObject(metalake, object, tagName));
    } catch (NoSuchTagException e) {
      LOG.info("Tag {} not found for object: {}", tagName, object);
      return Optional.empty();
    }
  }
}
