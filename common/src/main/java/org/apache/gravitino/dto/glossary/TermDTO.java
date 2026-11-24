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
import java.util.Collections;
import java.util.LinkedHashMap;
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

  /**
   * Creates a snapshot from one glossary revision.
   *
   * @param name The term name.
   * @param definition The optional definition.
   * @param synonyms The term synonyms.
   * @param status The term status.
   * @param domains The visible domain memberships.
   * @param broader The visible direct broader term names.
   * @param narrower The visible direct narrower term names.
   * @param related The visible direct related term names.
   * @param links The documentation links.
   * @param owner The optional owner.
   * @param sourceRef The optional source reference.
   * @param properties The term properties.
   * @param version The numeric revision version.
   * @param revisionToken The opaque identity and revision token.
   * @param audit The audit metadata.
   */
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
    this.synonyms = cloneArray(synonyms);
    this.status = status;
    this.domains = cloneArray(domains);
    this.broader = cloneArray(broader);
    this.narrower = cloneArray(narrower);
    this.related = cloneArray(related);
    this.links = cloneArray(links);
    this.owner = owner;
    this.sourceRef = sourceRef;
    this.properties =
        properties == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    this.version = version;
    this.revisionToken = revisionToken;
    this.audit = audit;
  }

  /**
   * @return A defensive copy of the synonyms array, or null if absent.
   */
  public String[] getSynonyms() {
    return cloneArray(synonyms);
  }

  /**
   * @return A defensive copy of the domains array, or null if absent.
   */
  public String[] getDomains() {
    return cloneArray(domains);
  }

  /**
   * @return A defensive copy of the broader terms array, or null if absent.
   */
  public String[] getBroader() {
    return cloneArray(broader);
  }

  /**
   * @return A defensive copy of the narrower terms array, or null if absent.
   */
  public String[] getNarrower() {
    return cloneArray(narrower);
  }

  /**
   * @return A defensive copy of the related terms array, or null if absent.
   */
  public String[] getRelated() {
    return cloneArray(related);
  }

  /**
   * @return A defensive copy of the documentation links array, or null if absent.
   */
  public TermLinkDTO[] getLinks() {
    return cloneArray(links);
  }

  private static <T> T[] cloneArray(@Nullable T[] values) {
    return values == null ? null : values.clone();
  }
}
