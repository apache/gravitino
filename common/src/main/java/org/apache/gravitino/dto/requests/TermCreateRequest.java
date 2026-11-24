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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.glossary.TermLinkDTO;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.rest.RESTRequest;

/** The initial definition, relations and memberships of a term. */
public class TermCreateRequest implements RESTRequest {
  @JsonProperty("name")
  private String name;

  @JsonProperty("definition")
  private @Nullable String definition;

  @JsonProperty("synonyms")
  private String[] synonyms = new String[0];

  @JsonProperty("status")
  @Nullable
  private String status;

  @JsonProperty("domains")
  private String[] domains = new String[0];

  @JsonProperty("broader")
  private String[] broader = new String[0];

  @JsonProperty("related")
  private String[] related = new String[0];

  @JsonProperty("links")
  private TermLinkDTO[] links = new TermLinkDTO[0];

  @JsonProperty("sourceRef")
  private @Nullable String sourceRef;

  @JsonProperty("properties")
  private Map<String, String> properties;

  /** Creates an empty request for deserialization. */
  public TermCreateRequest() {}

  /**
   * Creates a request from the client specification, preserving an omitted initial status.
   *
   * @param spec The term specification.
   */
  public TermCreateRequest(TermSpec spec) {
    name = spec.name();
    definition = spec.definition();
    synonyms = spec.synonyms();
    status = spec.status().map(Enum::name).orElse(null);
    domains = spec.domains();
    broader = spec.broader();
    related = spec.related();
    links =
        Arrays.stream(spec.links())
            .map(link -> new TermLinkDTO(link.label(), link.url()))
            .toArray(TermLinkDTO[]::new);
    sourceRef = spec.sourceRef().orElse(null);
    properties = spec.properties();
  }

  /**
   * Converts the wire representation to a validated term specification.
   *
   * @return The validated term specification.
   */
  public TermSpec toSpec() {
    Preconditions.checkArgument(links != null, "links must be an array");
    return TermSpec.builder(name)
        .withDefinition(definition)
        .withSynonyms(synonyms)
        .withDomains(domains)
        .withBroader(broader)
        .withRelated(related)
        .withStatus(status == null ? null : GlossaryTerm.Status.valueOf(status))
        .withSourceRef(sourceRef)
        .withProperties(properties)
        .withLinks(
            Arrays.stream(links)
                .map(
                    link -> {
                      Preconditions.checkArgument(link != null, "link must not be null");
                      return link.toLink();
                    })
                .toArray(TermLink[]::new))
        .build();
  }

  @Override
  public void validate() {
    toSpec();
  }
}
