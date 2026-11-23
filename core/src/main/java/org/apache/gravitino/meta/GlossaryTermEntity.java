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

package org.apache.gravitino.meta;

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.glossary.GlossaryTerm.Status;
import org.apache.gravitino.glossary.TermLink;

/**
 * Persisted glossary definition with its stored domain names. Relations and authorized API
 * projections are loaded separately.
 */
public final class GlossaryTermEntity implements Entity, HasIdentifier, Auditable {
  /** The id field. */
  public static final Field ID = Field.required("id", Long.class, "The glossary id.");
  /** The name field. */
  public static final Field NAME = Field.required("name", String.class, "The glossary name.");
  /** The definition field. */
  public static final Field DEFINITION =
      Field.optional("definition", String.class, "The glossary definition.");
  /** The synonyms field. */
  public static final Field SYNONYMS =
      Field.required("synonyms", String[].class, "The glossary synonyms.");
  /** The status field. */
  public static final Field STATUS = Field.required("status", Status.class, "The glossary status.");
  /** The links field. */
  public static final Field LINKS =
      Field.required("links", TermLink[].class, "The glossary links.");
  /** The sourceRef field. */
  public static final Field SOURCE_REF =
      Field.optional("sourceRef", String.class, "The glossary sourceRef.");
  /** The properties field. */
  public static final Field PROPERTIES =
      Field.required("properties", Map.class, "The glossary properties.");
  /** The version field. */
  public static final Field VERSION =
      Field.required("version", Long.class, "The glossary version.");
  /** The domains field, with the names of all stored domains, including hidden ones. */
  public static final Field DOMAINS =
      Field.required("domains", String[].class, "The glossary domains.");
  /** The auditInfo field. */
  public static final Field AUDIT_INFO =
      Field.required("auditInfo", Audit.class, "The glossary auditInfo.");

  private Long id;
  private String name;
  private Namespace namespace;
  @Nullable private String definition;
  private String[] synonyms;
  private Status status;
  private TermLink[] links;
  @Nullable private String sourceRef;
  private Map<String, String> properties;
  private long version;
  private Audit auditInfo;
  private String[] domains;

  private GlossaryTermEntity() {}
  /** Returns the id. */
  public Long id() {
    return id;
  }
  /** Returns the name. */
  public String name() {
    return name;
  }
  /** Returns the namespace. */
  public Namespace namespace() {
    return namespace;
  }
  /** Returns the definition. */
  @Nullable
  public String definition() {
    return definition;
  }
  /** Returns the synonyms. */
  public String[] synonyms() {
    return synonyms.clone();
  }
  /** Returns the status. */
  public Status status() {
    return status;
  }
  /** Returns the links. */
  public TermLink[] links() {
    return links.clone();
  }
  /** Returns the sourceRef. */
  @Nullable
  public String sourceRef() {
    return sourceRef;
  }
  /** Returns the properties. */
  public Map<String, String> properties() {
    return properties;
  }
  /** Returns the version. */
  public long version() {
    return version;
  }
  /**
   * Returns an opaque conditional-write token scoped to the metalake, term identity and revision.
   */
  public String revisionToken() {
    String identity = namespace.level(0) + ":" + id + ":" + version;
    return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
  }
  /** Returns the auditInfo. */
  public Audit auditInfo() {
    return auditInfo;
  }
  /**
   * Returns the names of the term's domains in ascending order. Storage reads return every stored
   * domain. Callers must filter hidden domains before returning a term to a user. Definition writes
   * ignore this value: membership changes only through the glossary term write path.
   */
  public String[] domains() {
    return domains.clone();
  }

  @Override
  public EntityType type() {
    return EntityType.GLOSSARY_TERM;
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> values = new LinkedHashMap<>();
    values.put(ID, id);
    values.put(NAME, name);
    values.put(DEFINITION, definition);
    values.put(SYNONYMS, synonyms.clone());
    values.put(STATUS, status);
    values.put(LINKS, links.clone());
    values.put(SOURCE_REF, sourceRef);
    values.put(PROPERTIES, properties);
    values.put(VERSION, version);
    values.put(AUDIT_INFO, auditInfo);
    values.put(DOMAINS, domains.clone());
    return Collections.unmodifiableMap(values);
  }
  /** Checks the UTF-8 storage bound for a proposed definition. */
  public static void validateDefinition(@Nullable String definition) {
    Preconditions.checkArgument(
        definition == null || definition.getBytes(StandardCharsets.UTF_8).length <= 16384,
        "Definition exceeds 16384 UTF-8 bytes");
  }

  /** Returns a definition builder. */
  public static Builder builder() {
    return new Builder();
  }
  /** Builds a persisted definition without relation projections. */
  public static final class Builder {
    private final GlossaryTermEntity value = new GlossaryTermEntity();

    private Builder() {
      value.synonyms = new String[0];
      value.links = new TermLink[0];
      value.status = Status.APPROVED;
      value.properties = Collections.emptyMap();
      value.version = 1;
      value.domains = new String[0];
    }
    /** Sets the id. */
    public Builder withId(Long v) {
      value.id = v;
      return this;
    }
    /** Sets the name. */
    public Builder withName(String v) {
      value.name = v;
      return this;
    }
    /** Sets the namespace. */
    public Builder withNamespace(Namespace v) {
      value.namespace = v;
      return this;
    }
    /** Sets the definition. */
    public Builder withDefinition(@Nullable String v) {
      value.definition = v;
      return this;
    }
    /** Sets the synonyms. */
    public Builder withSynonyms(String[] v) {
      Preconditions.checkArgument(v != null, "synonyms cannot be null");
      value.synonyms = v.clone();
      return this;
    }
    /** Sets the status. */
    public Builder withStatus(Status v) {
      value.status = v;
      return this;
    }
    /**
     * Sets the links.
     *
     * @throws IllegalArgumentException if the array or any link is null
     */
    public Builder withLinks(TermLink[] v) {
      Preconditions.checkArgument(v != null, "links cannot be null");
      TermLink[] links = v.clone();
      for (int i = 0; i < links.length; i++) {
        Preconditions.checkArgument(links[i] != null, "links[%s] cannot be null", i);
      }
      value.links = links;
      return this;
    }
    /** Sets the sourceRef. */
    public Builder withSourceRef(@Nullable String v) {
      value.sourceRef = v;
      return this;
    }
    /** Sets the properties. */
    public Builder withProperties(Map<String, String> v) {
      Preconditions.checkArgument(v != null, "properties cannot be null");
      value.properties = Collections.unmodifiableMap(new LinkedHashMap<>(v));
      return this;
    }
    /** Sets the version. */
    public Builder withVersion(long v) {
      value.version = v;
      return this;
    }
    /**
     * Sets the domain names, which are stored in ascending order.
     *
     * @throws IllegalArgumentException if the array is null
     */
    public Builder withDomains(String... v) {
      Preconditions.checkArgument(v != null, "domains cannot be null");
      String[] domains = v.clone();
      Arrays.sort(domains);
      value.domains = domains;
      return this;
    }
    /** Sets the auditInfo. */
    public Builder withAuditInfo(Audit v) {
      value.auditInfo = v;
      return this;
    }
    /** Returns the validated definition. */
    public GlossaryTermEntity build() {
      Preconditions.checkArgument(value.id != null, "id cannot be null");
      Preconditions.checkArgument(value.namespace != null, "namespace cannot be null");
      Preconditions.checkArgument(value.auditInfo != null, "auditInfo cannot be null");
      Preconditions.checkArgument(value.status != null, "status cannot be null");
      value.name = GlossaryTermNames.display(value.name);
      Preconditions.checkArgument(
          value.namespace.length() == 3
              && value.namespace.level(1).equals(Entity.SYSTEM_CATALOG_RESERVED_NAME)
              && value.namespace.level(2).equals("glossary_term"),
          "Expected glossary term namespace");
      GlossaryTermNames.validate(value.name);
      Preconditions.checkArgument(value.version >= 1, "Revision must be positive");
      Preconditions.checkArgument(value.synonyms.length <= 100, "At most 100 synonyms are allowed");
      for (int i = 0; i < value.synonyms.length; i++) {
        value.synonyms[i] = GlossaryTermNames.display(value.synonyms[i]);
        GlossaryTermNames.validate(value.synonyms[i]);
      }
      if (value.sourceRef != null) GlossaryTermNames.validate(value.sourceRef);
      validateDefinition(value.definition);
      GlossaryTermEntity copy = new GlossaryTermEntity();
      copy.id = value.id;
      copy.name = value.name;
      copy.namespace = value.namespace;
      copy.definition = value.definition;
      copy.synonyms = value.synonyms.clone();
      copy.status = value.status;
      copy.links = value.links.clone();
      copy.sourceRef = value.sourceRef;
      copy.properties = value.properties;
      copy.version = value.version;
      copy.auditInfo = value.auditInfo;
      copy.domains = value.domains.clone();
      return copy;
    }
  }
}
