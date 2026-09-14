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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA_AND_TAG;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationFullName;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.AuthorizationObjectType;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/tags")
public class MetadataObjectTagOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectTagOperations.class);

  private static final String TAG_VALUES_MEDIA_TYPE = "application/vnd.gravitino.v2+json";

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
  @AuthorizationExpression(
      expression = "METALAKE::OWNER || ((TAG::OWNER || ANY_APPLY_TAG) && CAN_ACCESS_METADATA)")
  public Response getTagForObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName,
      @PathParam("tag") @AuthorizationMetadata(type = Entity.EntityType.TAG) String tagName) {
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

            for (MetadataObject parentObject :
                MetadataObjectUtil.getParentMetadataObjects(object)) {
              if (tag.isPresent()) {
                break;
              }
              tag = getTagForObject(metalake, parentObject, tagName);
              tagDTO = tag.map(t -> DTOConverters.toDTO(t, Optional.of(true)));
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
  @AuthorizationExpression(expression = CAN_ACCESS_METADATA)
  public Response listTagsForMetadataObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName,
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

            Map<String, TagDTO> tags = new LinkedHashMap<>();
            Tag[] nonInheritedTags = tagDispatcher.listTagsInfoForMetadataObject(metalake, object);
            if (ArrayUtils.isNotEmpty(nonInheritedTags)) {
              Arrays.stream(nonInheritedTags)
                  .map(t -> DTOConverters.toDTO(t, Optional.of(false)))
                  .forEach(tag -> tags.putIfAbsent(tag.name(), tag));
            }

            for (MetadataObject parentObject :
                MetadataObjectUtil.getParentMetadataObjects(object)) {
              Tag[] inheritedTags =
                  tagDispatcher.listTagsInfoForMetadataObject(metalake, parentObject);
              if (ArrayUtils.isNotEmpty(inheritedTags)) {
                Arrays.stream(inheritedTags)
                    .map(t -> DTOConverters.toDTO(t, Optional.of(true)))
                    .forEach(tag -> tags.putIfAbsent(tag.name(), tag));
              }
            }

            if (verbose) {
              LOG.info(
                  "List {} tags info for object type: {}, full name: {} under metalake: {}",
                  tags.size(),
                  type,
                  fullName,
                  metalake);
              TagDTO[] tagDTOS = tags.values().toArray(new TagDTO[0]);
              tagDTOS =
                  MetadataAuthzHelper.filterByExpression(
                      metalake,
                      AuthorizationExpressionConstants.LOAD_TAG_AUTHORIZATION_EXPRESSION,
                      Entity.EntityType.TAG,
                      tagDTOS,
                      tagDTO -> NameIdentifierUtil.ofTag(metalake, tagDTO.name()));
              return Utils.ok(new TagListResponse(tagDTOS));

            } else {
              String[] tagNames = tags.keySet().toArray(new String[0]);
              tagNames =
                  MetadataAuthzHelper.filterByExpression(
                      metalake,
                      AuthorizationExpressionConstants.LOAD_TAG_AUTHORIZATION_EXPRESSION,
                      Entity.EntityType.TAG,
                      tagNames,
                      tagName -> NameIdentifierUtil.ofTag(metalake, tagName));
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
  @Consumes("application/json")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "associate-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-tags", absolute = true)
  @AuthorizationExpression(expression = CAN_ACCESS_METADATA_AND_TAG)
  public Response associateTagsForObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName,
      @AuthorizationRequest(type = AuthorizationRequest.RequestType.ASSOCIATE_TAG)
          TagsAssociateRequest request) {
    return associateTagsForObjectInternal(metalake, type, fullName, request);
  }

  /**
   * Associates tag values with a metadata object using the v2 request representation.
   *
   * @param metalake The metalake name.
   * @param type The metadata object type.
   * @param fullName The metadata object full name.
   * @param request The tag values association request.
   * @return The response containing associated tag names.
   */
  @POST
  @Consumes(TAG_VALUES_MEDIA_TYPE)
  @Produces(TAG_VALUES_MEDIA_TYPE)
  @Timed(name = "associate-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-tags", absolute = true)
  @AuthorizationExpression(expression = CAN_ACCESS_METADATA_AND_TAG)
  public Response associateTagValuesForObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName,
      @AuthorizationRequest(type = AuthorizationRequest.RequestType.ASSOCIATE_TAG)
          TagValuesAssociateRequest request) {
    return associateTagValuesForObjectInternal(metalake, type, fullName, request);
  }

  private Response associateTagsForObjectInternal(
      String metalake, String type, String fullName, TagsAssociateRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleTagException(
          OperationType.ASSOCIATE,
          "",
          fullName,
          new IllegalArgumentException("Request body cannot be null"));
    }

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
            MetadataObject object = parseMetadataObject(type, fullName);
            String[] tagNames =
                tagDispatcher.associateTagsForMetadataObject(
                    metalake, object, request.getTagsToAdd(), request.getTagsToRemove());
            tagNames = tagNames == null ? new String[0] : tagNames;
            logAssociatedTags(type, fullName, metalake, tagNames);
            return Utils.ok(new NameListResponse(tagNames));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.ASSOCIATE, "", fullName, e);
    }
  }

  private Response associateTagValuesForObjectInternal(
      String metalake, String type, String fullName, TagValuesAssociateRequest request) {
    if (request == null) {
      return withMediaType(
          ExceptionHandlers.handleTagException(
              OperationType.ASSOCIATE,
              "",
              fullName,
              new IllegalArgumentException("Request body cannot be null")),
          TAG_VALUES_MEDIA_TYPE);
    }

    LOG.info(
        "Received associate tag values request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object = parseMetadataObject(type, fullName);
            String[] tagNames =
                tagDispatcher.associateTagValuesForMetadataObject(
                    metalake, object, request.tagValuesToAdd(), request.tagValuesToRemove());
            tagNames = tagNames == null ? new String[0] : tagNames;
            logAssociatedTags(type, fullName, metalake, tagNames);
            return Response.ok(new NameListResponse(tagNames), TAG_VALUES_MEDIA_TYPE).build();
          });
    } catch (Exception e) {
      return withMediaType(
          ExceptionHandlers.handleTagException(OperationType.ASSOCIATE, "", fullName, e),
          TAG_VALUES_MEDIA_TYPE);
    }
  }

  private static MetadataObject parseMetadataObject(String type, String fullName) {
    return MetadataObjects.parse(
        fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
  }

  private static void logAssociatedTags(
      String type, String fullName, String metalake, String[] tagNames) {
    LOG.info(
        "Associated tags: {} for object type: {}, full name: {} under metalake: {}",
        Arrays.toString(tagNames),
        type,
        fullName,
        metalake);
  }

  private static Response withMediaType(Response response, String mediaType) {
    return Response.fromResponse(response).type(mediaType).build();
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
