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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.kb.ArticleBodyDTO;
import org.apache.gravitino.dto.kb.ArticleObjectDTO;
import org.apache.gravitino.json.StrictJsonStringDeserializer;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.rest.RESTRequest;

/** Fields and initial bindings for creating a Knowledge Base Article. */
public class ArticleCreateRequest implements RESTRequest {
  @JsonProperty("name")
  @Nullable
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String name;

  @JsonProperty("title")
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String title;

  @JsonProperty("body")
  private ArticleBodyDTO body;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("status")
  @JsonSetter(nulls = Nulls.FAIL)
  @Nullable
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String status;

  @JsonProperty("domains")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
  private String[] domains = new String[0];

  @JsonProperty("terms")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
  private String[] terms = new String[0];

  @JsonProperty("objects")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  private ArticleObjectDTO[] objects = new ArticleObjectDTO[0];

  @JsonProperty("comment")
  @Nullable
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String comment;

  @JsonProperty("synonyms")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
  private String[] synonyms = new String[0];

  @JsonProperty("sourceReference")
  @Nullable
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String sourceReference;

  @JsonProperty("properties")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
  private Map<String, String> properties = Collections.emptyMap();

  /**
   * @param create the Article to create, including its initial bindings
   * @return the corresponding REST request
   */
  public static ArticleCreateRequest from(ArticleCreate create) {
    ArticleCreateRequest request = new ArticleCreateRequest();
    request.name = create.name();
    request.title = create.title();
    request.body = ArticleBodyDTO.from(create.body());
    request.status = create.status() == null ? null : create.status().name();
    request.domains = create.domains().toArray(new String[0]);
    request.terms = create.terms().toArray(new String[0]);
    request.objects = ArticleObjectDTO.fromIdentifiers(create.objects());
    request.comment = create.comment();
    request.synonyms = create.synonyms().toArray(new String[0]);
    request.sourceReference = create.sourceReference();
    request.properties = create.properties();
    return request;
  }

  /**
   * Creates a validated Article specification.
   *
   * @return the initial Article fields and bindings
   */
  public ArticleCreate toCreate() {
    Preconditions.checkArgument(body != null, "body is required");
    return ArticleCreate.builder()
        .withName(name)
        .withTitle(title)
        .withBody(body.toBody())
        .withStatus(status == null ? null : ArticleStatus.valueOf(status.toUpperCase(Locale.ROOT)))
        .addDomains(domains)
        .addTerms(terms)
        .withComment(comment)
        .addSynonyms(synonyms)
        .withSourceReference(sourceReference)
        .withProperties(properties)
        .addObjects(ArticleObjectDTO.toIdentifiers(objects))
        .build();
  }

  @Override
  public void validate() {
    toCreate();
  }
}
