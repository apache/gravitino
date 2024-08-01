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
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.TagCreateRequest;
import org.apache.gravitino.dto.requests.TagUpdateRequest;
import org.apache.gravitino.dto.requests.TagUpdatesRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/tags")
public class TagOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TagOperations.class);

  private final TagManager tagManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TagOperations(TagManager tagManager) {
    this.tagManager = tagManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-tags", absolute = true)
  public Response listTags(
      @PathParam("metalake") String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list tag {} request for metalake: {}", verbose ? "infos" : "names", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (verbose) {
              Tag[] tags = tagManager.listTagsInfo(metalake);
              TagDTO[] tagDTOs;
              if (ArrayUtils.isEmpty(tags)) {
                tagDTOs = new TagDTO[0];
              } else {
                tagDTOs =
                    Arrays.stream(tags)
                        .map(t -> DTOConverters.toDTO(t, Optional.empty()))
                        .toArray(TagDTO[]::new);
              }

              LOG.info("List {} tags info under metalake: {}", tagDTOs.length, metalake);
              return Utils.ok(new TagListResponse(tagDTOs));

            } else {
              String[] tagNames = tagManager.listTags(metalake);
              tagNames = tagNames == null ? new String[0] : tagNames;

              LOG.info("List {} tags under metalake: {}", tagNames.length, metalake);
              return Utils.ok(new NameListResponse(tagNames));
            }
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-tag", absolute = true)
  public Response createTag(@PathParam("metalake") String metalake, TagCreateRequest request) {
    LOG.info("Received create tag request under metalake: {}", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            Tag tag =
                tagManager.createTag(
                    metalake, request.getName(), request.getComment(), request.getProperties());

            LOG.info("Created tag: {} under metalake: {}", tag.name(), metalake);
            return Utils.ok(new TagResponse(DTOConverters.toDTO(tag, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @GET
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-tag", absolute = true)
  public Response getTag(@PathParam("metalake") String metalake, @PathParam("tag") String name) {
    LOG.info("Received get tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Tag tag = tagManager.getTag(metalake, name);
            LOG.info("Get tag: {} under metalake: {}", name, metalake);
            return Utils.ok(new TagResponse(DTOConverters.toDTO(tag, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.GET, name, metalake, e);
    }
  }

  @PUT
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-tag", absolute = true)
  public Response alterTag(
      @PathParam("metalake") String metalake,
      @PathParam("tag") String name,
      TagUpdatesRequest request) {
    LOG.info("Received alter tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            TagChange[] changes =
                request.getUpdates().stream()
                    .map(TagUpdateRequest::tagChange)
                    .toArray(TagChange[]::new);
            Tag tag = tagManager.alterTag(metalake, name, changes);

            LOG.info("Altered tag: {} under metalake: {}", name, metalake);
            return Utils.ok(new TagResponse(DTOConverters.toDTO(tag, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.ALTER, name, metalake, e);
    }
  }

  @DELETE
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-tag", absolute = true)
  public Response deleteTag(@PathParam("metalake") String metalake, @PathParam("tag") String name) {
    LOG.info("Received delete tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = tagManager.deleteTag(metalake, name);
            if (!deleted) {
              LOG.warn("Failed to delete tag {} under metalake {}", name, metalake);
            } else {
              LOG.info("Deleted tag: {} under metalake: {}", name, metalake);
            }

            return Utils.ok(new DropResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.DELETE, name, metalake, e);
    }
  }

  @GET
  @Path("{type}/{fullName}")
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

            List<TagDTO> tags = Lists.newArrayList();
            Tag[] nonInheritedTags = tagManager.listTagsInfoForMetadataObject(metalake, object);
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
                  tagManager.listTagsInfoForMetadataObject(metalake, parentObject);
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
              // Due to same name tag will be associated to both parent and child objects, so we
              // need to deduplicate the tag names.
              String[] tagNames = tags.stream().map(TagDTO::name).distinct().toArray(String[]::new);

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

  @GET
  @Path("{type}/{fullName}/{tag}")
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
  @Path("{tag}/objects")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-objects-for-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-objects-for-tag", absolute = true)
  public Response listMetadataObjectsForTag(
      @PathParam("metalake") String metalake, @PathParam("tag") String tagName) {
    LOG.info("Received list objects for tag: {} under metalake: {}", tagName, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject[] objects = tagManager.listMetadataObjectsForTag(metalake, tagName);
            objects = objects == null ? new MetadataObject[0] : objects;

            LOG.info(
                "List {} objects for tag: {} under metalake: {}",
                objects.length,
                tagName,
                metalake);

            MetadataObjectDTO[] objectDTOs =
                Arrays.stream(objects).map(DTOConverters::toDTO).toArray(MetadataObjectDTO[]::new);
            return Utils.ok(new MetadataObjectListResponse(objectDTOs));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", tagName, e);
    }
  }

  @POST
  @Path("{type}/{fullName}")
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
                tagManager.associateTagsForMetadataObject(
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
      return Optional.ofNullable(tagManager.getTagForMetadataObject(metalake, object, tagName));
    } catch (NoSuchTagException e) {
      LOG.info("Tag {} not found for object: {}", tagName, object);
      return Optional.empty();
    }
  }
}
