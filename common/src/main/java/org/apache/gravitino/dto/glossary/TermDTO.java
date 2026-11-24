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
package org.apache.gravitino.dto.glossary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.glossary.GlossaryTerm;

/** A glossary definition with its visible memberships and direct relation names. */
@Getter
public class TermDTO {
  private final String name;
  private final @Nullable String definition;
  private final String[] synonyms;

  @JsonSerialize(using = ToStringSerializer.class)
  private final GlossaryTerm.Status status;

  private final String[] domains;
  private final String[] broader;
  private final String[] narrower;
  private final String[] related;
  private final TermLinkDTO[] links;
  private final @Nullable OwnerDTO owner;
  private final @Nullable String sourceRef;
  private final Map<String, String> properties;
  private final long version;
  private final String revisionToken;
  private final AuditDTO audit;

  /** Creates a snapshot from one glossary revision. */
  @JsonCreator
  public TermDTO(
      @JsonProperty("name") String name,
      @JsonProperty("definition") @Nullable String definition,
      @JsonProperty("synonyms") String[] synonyms,
      @JsonProperty("status") GlossaryTerm.Status status,
      @JsonProperty("domains") String[] domains,
      @JsonProperty("broader") String[] broader,
      @JsonProperty("narrower") String[] narrower,
      @JsonProperty("related") String[] related,
      @JsonProperty("links") TermLinkDTO[] links,
      @JsonProperty("owner") @Nullable OwnerDTO owner,
      @JsonProperty("sourceRef") @Nullable String sourceRef,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("version") long version,
      @JsonProperty("revisionToken") String revisionToken,
      @JsonProperty("audit") AuditDTO audit) {
    this.name = name;
    this.definition = definition;
    this.synonyms = synonyms;
    this.status = status;
    this.domains = domains;
    this.broader = broader;
    this.narrower = narrower;
    this.related = related;
    this.links = links;
    this.owner = owner;
    this.sourceRef = sourceRef;
    this.properties = properties;
    this.version = version;
    this.revisionToken = revisionToken;
    this.audit = audit;
  }
}
