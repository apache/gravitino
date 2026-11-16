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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TAG_AUTHORIZATION_EXPRESSION;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.dto.requests.BulkMetadataObjectsRequest;
import org.apache.gravitino.dto.responses.BulkMetadataObjectTagListResponse;
import org.apache.gravitino.dto.responses.BulkMetadataObjectTagListResponse.Result;
import org.apache.gravitino.dto.responses.MetadataObjectTagListResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Provides bounded, read-only bulk lookups of effective tags. */
@Path("/bulk/metalakes/{metalake}/objects/tags")
public class BulkMetadataObjectTagOperations {
  private final TagDispatcher tagDispatcher;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the bulk tag resource.
   *
   * @param tagDispatcher the tag dispatcher
   */
  @Inject
  public BulkMetadataObjectTagOperations(TagDispatcher tagDispatcher) {
    this.tagDispatcher = tagDispatcher;
  }

  /**
   * Lists effective tags in request order, preserving the single-object API's visibility rules.
   * Registered tables and schemas use their stored identities without reloading external metadata.
   *
   * @param metalake the metalake name
   * @param request the metadata objects to load
   * @return successful tags results and per-object errors
   */
  @POST
  @Consumes("application/json")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-tags-bulk." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-tags-bulk", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listTags(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkMetadataObjectsRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Preconditions.checkArgument(request != null, "Request body cannot be null");
            request.validate();
            BulkManager manager = new BulkManager(GravitinoEnv.getInstance().config());
            manager.checkBulkSize("metadataObjects", request.getMetadataObjects().length);
            List<BulkItemResult<Result>> results =
                BulkMetadataObjectReader.read(
                    metalake,
                    request.getMetadataObjects(),
                    objects -> tagDispatcher.listEffectiveTagsForDisplay(metalake, objects),
                    (i, tags) -> {
                      TagDTO[] allTags =
                          Arrays.stream(tags)
                              .map(
                                  tag ->
                                      DTOConverters.toDTO(
                                          tag, Optional.of(tag.inherited().orElse(false))))
                              .toArray(TagDTO[]::new);
                      TagDTO[] visibleTags =
                          MetadataAuthzHelper.filterByExpression(
                              metalake,
                              LOAD_TAG_AUTHORIZATION_EXPRESSION,
                              Entity.EntityType.TAG,
                              allTags,
                              tag -> NameIdentifierUtil.ofTag(metalake, tag.name()));
                      return new Result(
                          request.getMetadataObjects()[i],
                          new MetadataObjectTagListResponse(
                              visibleTags, visibleTags.length < allTags.length));
                    });
            return Utils.ok(
                new BulkMetadataObjectTagListResponse(
                    results.stream()
                        .filter(BulkItemResult::succeeded)
                        .map(result -> result.value().orElseThrow())
                        .toArray(Result[]::new),
                    BulkMetadataObjectReader.errors(results, manager)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", metalake, e);
    }
  }
}
