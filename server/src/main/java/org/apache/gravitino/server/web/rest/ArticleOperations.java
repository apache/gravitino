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
import com.google.common.base.Preconditions;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.dto.kb.ArticleDTO;
import org.apache.gravitino.dto.kb.ArticleObjectDTO;
import org.apache.gravitino.dto.kb.ArticleTermDTO;
import org.apache.gravitino.dto.requests.ArticleCreateRequest;
import org.apache.gravitino.dto.requests.ArticleUpdatesRequest;
import org.apache.gravitino.dto.responses.ArticleAssetBindingsResponse;
import org.apache.gravitino.dto.responses.ArticleContentResponse;
import org.apache.gravitino.dto.responses.ArticleResponse;
import org.apache.gravitino.dto.responses.ArticleTermBindingsResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.kb.ArticleManager;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;

/**
 * Metalake-scoped Knowledge Base Articles. The interceptor checks metalake membership, and the
 * Article manager checks visibility and mutation privileges against the stored Article and targets.
 */
@Path("/metalakes/{metalake}/kb-articles")
@Produces({"application/vnd.gravitino.v1+json", MediaType.APPLICATION_JSON})
public class ArticleOperations {
  private static final ExceptionHandler COMMON_ERRORS =
      new ExceptionHandlers.BaseExceptionHandler();
  private final ArticleManager manager;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the Article resource backed by the server's manager.
   *
   * @param manager the Article manager
   */
  @Inject
  public ArticleOperations(ArticleManager manager) {
    this.manager = manager;
  }

  /**
   * Creates an Article with its initial content, Domains and bindings.
   *
   * @param metalake the metalake name
   * @param request the initial Article definition
   * @return the created metadata and editing token
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Timed(name = "create-article." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-article", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response createArticle(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      ArticleCreateRequest request) {
    return execute(
        OperationType.CREATE,
        metalake,
        null,
        () -> {
          Preconditions.checkArgument(request != null, "Request body cannot be null");
          return metadata(manager.createArticle(metalake, request.toCreate()));
        });
  }

  /**
   * Returns Article metadata with independently bounded Term and asset previews.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @param limit maximum number of readable bindings in each preview
   * @param offset number of readable bindings to skip in each preview
   * @return the metadata and editing token
   */
  @GET
  @Path("{article}")
  @Timed(name = "get-article." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-article", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getArticle(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article,
      @QueryParam("limit") @DefaultValue("20") String limit,
      @QueryParam("offset") @DefaultValue("0") String offset) {
    return execute(
        OperationType.GET,
        metalake,
        article,
        () ->
            metadata(
                manager.loadArticle(
                    metalake, article, Integer.parseInt(limit), Integer.parseInt(offset))));
  }

  /**
   * Returns the stored body and its editing token from the same snapshot.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @return the current body and editing token
   */
  @GET
  @Path("{article}/content")
  @Timed(name = "get-article-content." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-article-content", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getArticleContent(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article) {
    return execute(
        OperationType.GET,
        metalake,
        article,
        () -> Utils.ok(new ArticleContentResponse(manager.getArticleContent(metalake, article))));
  }

  /**
   * Lists the live, readable Terms bound to an Article.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @param limit maximum number of readable Terms to return
   * @param offset number of readable Terms to skip
   * @return the binding page
   */
  @GET
  @Path("{article}/terms")
  @Timed(name = "list-article-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-article-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listTermBindings(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article,
      @QueryParam("limit") @DefaultValue("20") String limit,
      @QueryParam("offset") @DefaultValue("0") String offset) {
    return execute(
        OperationType.LIST,
        metalake,
        article,
        () ->
            Utils.ok(
                new ArticleTermBindingsResponse(
                    manager.listTermBindings(
                        metalake, article, Integer.parseInt(limit), Integer.parseInt(offset)))));
  }

  /**
   * Lists the live, readable assets bound to an Article.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @param limit maximum number of readable assets to return
   * @param offset number of readable assets to skip
   * @return the binding page
   */
  @GET
  @Path("{article}/objects")
  @Timed(name = "list-article-objects." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-article-objects", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listAssetBindings(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article,
      @QueryParam("limit") @DefaultValue("20") String limit,
      @QueryParam("offset") @DefaultValue("0") String offset) {
    return execute(
        OperationType.LIST,
        metalake,
        article,
        () ->
            Utils.ok(
                new ArticleAssetBindingsResponse(
                    manager.listAssetBindings(
                        metalake, article, Integer.parseInt(limit), Integer.parseInt(offset)))));
  }

  /**
   * Applies edits atomically and checks the supplied revision when present.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @param request the changes and optional editing token
   * @return the updated metadata and editing token
   */
  @PUT
  @Path("{article}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Timed(name = "alter-article." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-article", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response alterArticle(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article,
      ArticleUpdatesRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        article,
        () -> {
          Preconditions.checkArgument(request != null, "Request body cannot be null");
          return metadata(
              manager.alterArticle(
                  metalake, article, request.getExpectedRevision(), request.changes()));
        });
  }

  /**
   * Deletes the Article and returns dropped=false for an absent Article or a hidden draft.
   *
   * @param metalake the metalake name
   * @param article the Article name
   * @return the drop result
   */
  @DELETE
  @Path("{article}")
  @Timed(name = "delete-article." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-article", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response deleteArticle(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("article") String article) {
    return execute(
        OperationType.DELETE,
        metalake,
        article,
        () -> Utils.ok(new DropResponse(manager.deleteArticle(metalake, article))));
  }

  private static Response metadata(ArticleManager.Result result) {
    var article = result.article();
    return Utils.ok(
        new ArticleResponse(
            new ArticleDTO(
                article.name(),
                article.title(),
                article.status(),
                article.domains(),
                article.comment(),
                article.synonyms(),
                article.sourceReference(),
                article.properties(),
                article.terms().stream().map(ArticleTermDTO::from).toList(),
                article.termsHasMore(),
                article.objects().stream().map(ArticleObjectDTO::from).toList(),
                article.objectsHasMore(),
                article.owner() == null ? null : DTOConverters.toDTO(article.owner()),
                result.revisionToken(),
                DTOConverters.toDTO(article.auditInfo()))));
  }

  private Response execute(
      OperationType operation,
      String metalake,
      @Nullable String article,
      Supplier<Response> action) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetalakeManager.checkMetalakeInUse(metalake);
            return action.get();
          });
    } catch (IllegalArgumentException e) {
      return Utils.illegalArguments(e.getMessage(), e);
    } catch (NotFoundException e) {
      return Utils.notFound(e.getMessage(), e);
    } catch (ForbiddenException e) {
      return Utils.forbidden(e.getMessage(), e);
    } catch (NotInUseException e) {
      return Utils.notInUse(e.getMessage(), e);
    } catch (AlreadyExistsException e) {
      return Utils.alreadyExists(e.getMessage(), e);
    } catch (UnapprovedTermException e) {
      return Utils.operationConflict(e.getMessage(), e);
    } catch (Exception e) {
      return COMMON_ERRORS.handle(operation, article, metalake, e);
    }
  }
}
