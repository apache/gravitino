/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.requests.ArticleCreateRequest;
import org.apache.gravitino.dto.requests.ArticleUpdatesRequest;
import org.apache.gravitino.dto.responses.ArticleAssetBindingsResponse;
import org.apache.gravitino.dto.responses.ArticleContentResponse;
import org.apache.gravitino.dto.responses.ArticleListResponse;
import org.apache.gravitino.dto.responses.ArticleResponse;
import org.apache.gravitino.dto.responses.ArticleTermBindingsResponse;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.kb.ArticleContent;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticlePage;
import org.apache.gravitino.kb.AssetBindingPage;
import org.apache.gravitino.kb.KnowledgeBaseArticle;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.apache.gravitino.kb.KnowledgeBaseArticleOperations;
import org.apache.gravitino.kb.TermBindingPage;
import org.apache.gravitino.rest.RESTUtils;

/** REST-backed Knowledge Base Article operations for a single metalake. */
final class ArticleOperations implements KnowledgeBaseArticleOperations {
  private final String path;
  private final RESTClient client;

  ArticleOperations(String metalake, RESTClient client) {
    this.path = "api/metalakes/" + RESTUtils.encodeString(metalake) + "/kb-articles";
    this.client = client;
  }

  @Override
  public ArticlePage listArticles(ArticleListFilter filter) {
    Preconditions.checkArgument(filter != null, "filter must not be null");
    Map<String, String> params = pagination(filter.limit(), filter.offset());
    if (filter.query() != null) {
      params.put("q", filter.query());
    }
    if (filter.domain() != null) {
      params.put("domain", filter.domain());
    }
    if (filter.status() != null) {
      params.put("status", filter.status().name());
    }
    if (filter.owner() != null) {
      params.put(
          "owner",
          filter.owner().type().name().toLowerCase(Locale.ROOT) + ":" + filter.owner().name());
    }
    if (filter.term() != null) {
      params.put("term", filter.term());
    }
    if (filter.includeDeprecated()) {
      params.put("includeDeprecated", "true");
    }
    String requestPath = path;
    ArticleObjectIdentifier object = filter.object();
    if (object != null) {
      params.put("objectType", object.type().name());
      params.put("name", object.name());
      if (!object.namespace().isEmpty()) {
        // Each namespace component is a separate query parameter, including components with dots.
        requestPath +=
            object.namespace().stream()
                .map(component -> "namespace=" + RESTUtils.encodeString(component))
                .collect(Collectors.joining("&", "?", ""));
      }
    }
    ArticleListResponse response =
        client.get(
            requestPath,
            params,
            ArticleListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.toPage();
  }

  @Override
  public KnowledgeBaseArticle loadArticle(String name, int limit, int offset) {
    ArticleResponse response =
        client.get(
            articlePath(name),
            pagination(limit, offset),
            ArticleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.getArticle();
  }

  @Override
  public ArticleContent getArticleContent(String name) {
    ArticleContentResponse response =
        client.get(
            articlePath(name) + "/content",
            ArticleContentResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.toContent();
  }

  @Override
  public KnowledgeBaseArticle createArticle(ArticleCreate articleCreate) {
    ArticleCreateRequest request = ArticleCreateRequest.from(articleCreate);
    request.validate();
    ArticleResponse response =
        client.post(
            path,
            request,
            ArticleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.getArticle();
  }

  @Override
  public KnowledgeBaseArticle alterArticle(
      String name, KnowledgeBaseArticleChange[] changes, @Nullable String expectedRevision) {
    ArticleUpdatesRequest request = ArticleUpdatesRequest.from(changes, expectedRevision);
    request.validate();
    ArticleResponse response =
        client.put(
            articlePath(name),
            request,
            ArticleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.getArticle();
  }

  @Override
  public boolean deleteArticle(String name) {
    DropResponse response =
        client.delete(
            articlePath(name),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.dropped();
  }

  @Override
  public void detachDomain(String name, String domain) {
    BaseResponse response =
        client.delete(
            articlePath(name) + "/domains/" + RESTUtils.encodeDomainPathSegment(domain),
            BaseResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
  }

  @Override
  public AssetBindingPage listAssetBindings(String name, int limit, int offset) {
    ArticleAssetBindingsResponse response =
        client.get(
            articlePath(name) + "/objects",
            pagination(limit, offset),
            ArticleAssetBindingsResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.toPage();
  }

  @Override
  public TermBindingPage listTermBindings(String name, int limit, int offset) {
    ArticleTermBindingsResponse response =
        client.get(
            articlePath(name) + "/terms",
            pagination(limit, offset),
            ArticleTermBindingsResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.articleErrorHandler());
    response.validate();
    return response.toPage();
  }

  private String articlePath(String name) {
    return path + "/" + RESTUtils.encodeString(KnowledgeBaseArticle.normalizeName(name));
  }

  private static Map<String, String> pagination(int limit, int offset) {
    Map<String, String> params = new HashMap<>();
    params.put("limit", Integer.toString(limit));
    params.put("offset", Integer.toString(offset));
    return params;
  }
}
