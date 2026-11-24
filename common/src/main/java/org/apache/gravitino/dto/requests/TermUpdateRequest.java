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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.glossary.TermLinkDTO;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.glossary.TermLink;

/** One typed edit in an atomic glossary update. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = TermUpdateRequest.Rename.class, name = "rename"),
  @JsonSubTypes.Type(value = TermUpdateRequest.UpdateDefinition.class, name = "updateDefinition"),
  @JsonSubTypes.Type(value = TermUpdateRequest.SetStatus.class, name = "setStatus"),
  @JsonSubTypes.Type(value = TermUpdateRequest.AddSynonym.class, name = "addSynonym"),
  @JsonSubTypes.Type(value = TermUpdateRequest.RemoveSynonym.class, name = "removeSynonym"),
  @JsonSubTypes.Type(value = TermUpdateRequest.AddDomain.class, name = "addDomain"),
  @JsonSubTypes.Type(value = TermUpdateRequest.RemoveDomain.class, name = "removeDomain"),
  @JsonSubTypes.Type(value = TermUpdateRequest.AddBroader.class, name = "addBroader"),
  @JsonSubTypes.Type(value = TermUpdateRequest.RemoveBroader.class, name = "removeBroader"),
  @JsonSubTypes.Type(value = TermUpdateRequest.AddRelated.class, name = "addRelated"),
  @JsonSubTypes.Type(value = TermUpdateRequest.RemoveRelated.class, name = "removeRelated"),
  @JsonSubTypes.Type(value = TermUpdateRequest.UpdateLinks.class, name = "updateLinks"),
  @JsonSubTypes.Type(value = TermUpdateRequest.SetSourceRef.class, name = "setSourceRef"),
  @JsonSubTypes.Type(value = TermUpdateRequest.SetProperty.class, name = "setProperty"),
  @JsonSubTypes.Type(value = TermUpdateRequest.RemoveProperty.class, name = "removeProperty")
})
public interface TermUpdateRequest {
  /**
   * Returns the API change represented by this update.
   *
   * @return The API term change.
   */
  TermChange toChange();

  /** The rename operation. */
  class Rename implements TermUpdateRequest {
    @JsonProperty("newName")
    private final String newName;

    /**
     * Creates the rename update.
     *
     * @param newName The new term name.
     */
    @JsonCreator
    public Rename(@JsonProperty(value = "newName", required = true) String newName) {
      this.newName = newName;
    }

    @Override
    public TermChange toChange() {
      return TermChange.rename(newName);
    }
  }

  /** The updateDefinition operation. */
  class UpdateDefinition implements TermUpdateRequest {
    @Nullable
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonProperty("newDefinition")
    private final String newDefinition;

    /**
     * Creates the updateDefinition update. An explicit null clears the value.
     *
     * @param newDefinition The new definition, or null to clear it.
     */
    @JsonCreator
    public UpdateDefinition(
        @JsonProperty(value = "newDefinition", required = true) @Nullable String newDefinition) {
      this.newDefinition = newDefinition;
    }

    @Override
    public TermChange toChange() {
      return TermChange.updateDefinition(newDefinition);
    }
  }

  /** The setStatus operation. */
  class SetStatus implements TermUpdateRequest {
    @JsonProperty("status")
    private final String status;

    /**
     * Creates the setStatus update.
     *
     * @param status The target term status.
     */
    @JsonCreator
    public SetStatus(@JsonProperty(value = "status", required = true) String status) {
      this.status = status;
    }

    @Override
    public TermChange toChange() {
      Preconditions.checkArgument(status != null, "status is required");
      return TermChange.setStatus(GlossaryTerm.Status.valueOf(status));
    }
  }

  /** The addSynonym operation. */
  class AddSynonym implements TermUpdateRequest {
    @JsonProperty("synonym")
    private final String synonym;

    /**
     * Creates the addSynonym update.
     *
     * @param synonym The synonym to add.
     */
    @JsonCreator
    public AddSynonym(@JsonProperty(value = "synonym", required = true) String synonym) {
      this.synonym = synonym;
    }

    @Override
    public TermChange toChange() {
      return TermChange.addSynonym(synonym);
    }
  }

  /** The removeSynonym operation. */
  class RemoveSynonym implements TermUpdateRequest {
    @JsonProperty("synonym")
    private final String synonym;

    /**
     * Creates the removeSynonym update.
     *
     * @param synonym The synonym to remove.
     */
    @JsonCreator
    public RemoveSynonym(@JsonProperty(value = "synonym", required = true) String synonym) {
      this.synonym = synonym;
    }

    @Override
    public TermChange toChange() {
      return TermChange.removeSynonym(synonym);
    }
  }

  /** The addDomain operation. */
  class AddDomain implements TermUpdateRequest {
    @JsonProperty("domain")
    private final String domain;

    /**
     * Creates the addDomain update.
     *
     * @param domain The domain name to add.
     */
    @JsonCreator
    public AddDomain(@JsonProperty(value = "domain", required = true) String domain) {
      this.domain = domain;
    }

    @Override
    public TermChange toChange() {
      return TermChange.addDomain(domain);
    }
  }

  /** The removeDomain operation. */
  class RemoveDomain implements TermUpdateRequest {
    @JsonProperty("domain")
    private final String domain;

    /**
     * Creates the removeDomain update.
     *
     * @param domain The domain name to remove.
     */
    @JsonCreator
    public RemoveDomain(@JsonProperty(value = "domain", required = true) String domain) {
      this.domain = domain;
    }

    @Override
    public TermChange toChange() {
      return TermChange.removeDomain(domain);
    }
  }

  /** The addBroader operation. */
  class AddBroader implements TermUpdateRequest {
    @JsonProperty("broader")
    private final String broader;

    /**
     * Creates the addBroader update.
     *
     * @param broader The broader term name to add.
     */
    @JsonCreator
    public AddBroader(@JsonProperty(value = "broader", required = true) String broader) {
      this.broader = broader;
    }

    @Override
    public TermChange toChange() {
      return TermChange.addBroader(broader);
    }
  }

  /** The removeBroader operation. */
  class RemoveBroader implements TermUpdateRequest {
    @JsonProperty("broader")
    private final String broader;

    /**
     * Creates the removeBroader update.
     *
     * @param broader The broader term name to remove.
     */
    @JsonCreator
    public RemoveBroader(@JsonProperty(value = "broader", required = true) String broader) {
      this.broader = broader;
    }

    @Override
    public TermChange toChange() {
      return TermChange.removeBroader(broader);
    }
  }

  /** The addRelated operation. */
  class AddRelated implements TermUpdateRequest {
    @JsonProperty("related")
    private final String related;

    /**
     * Creates the addRelated update.
     *
     * @param related The related term name to add.
     */
    @JsonCreator
    public AddRelated(@JsonProperty(value = "related", required = true) String related) {
      this.related = related;
    }

    @Override
    public TermChange toChange() {
      return TermChange.addRelated(related);
    }
  }

  /** The removeRelated operation. */
  class RemoveRelated implements TermUpdateRequest {
    @JsonProperty("related")
    private final String related;

    /**
     * Creates the removeRelated update.
     *
     * @param related The related term name to remove.
     */
    @JsonCreator
    public RemoveRelated(@JsonProperty(value = "related", required = true) String related) {
      this.related = related;
    }

    @Override
    public TermChange toChange() {
      return TermChange.removeRelated(related);
    }
  }

  /** The updateLinks operation. */
  class UpdateLinks implements TermUpdateRequest {
    @JsonProperty("links")
    private final TermLinkDTO[] links;

    /**
     * Creates the updateLinks update.
     *
     * @param links The replacement documentation links.
     */
    @JsonCreator
    public UpdateLinks(@JsonProperty(value = "links", required = true) TermLinkDTO[] links) {
      this.links = links;
    }

    @Override
    public TermChange toChange() {
      Preconditions.checkArgument(
          links != null && Arrays.stream(links).allMatch(Objects::nonNull),
          "links must contain only links");
      return TermChange.updateLinks(
          Arrays.stream(links).map(TermLinkDTO::toLink).toArray(TermLink[]::new));
    }
  }

  /** The setSourceRef operation. */
  class SetSourceRef implements TermUpdateRequest {
    @Nullable
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonProperty("sourceRef")
    private final String sourceRef;

    /**
     * Creates the setSourceRef update. An explicit null clears the value.
     *
     * @param sourceRef The new source reference, or null to clear it.
     */
    @JsonCreator
    public SetSourceRef(
        @JsonProperty(value = "sourceRef", required = true) @Nullable String sourceRef) {
      this.sourceRef = sourceRef;
    }

    @Override
    public TermChange toChange() {
      return TermChange.setSourceRef(sourceRef);
    }
  }

  /** The setProperty operation. */
  class SetProperty implements TermUpdateRequest {
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /**
     * Creates the setProperty update.
     *
     * @param property The property name.
     * @param value The property value.
     */
    @JsonCreator
    public SetProperty(
        @JsonProperty(value = "property", required = true) String property,
        @JsonProperty(value = "value", required = true) String value) {
      this.property = property;
      this.value = value;
    }

    @Override
    public TermChange toChange() {
      return TermChange.setProperty(property, value);
    }
  }

  /** The removeProperty operation. */
  class RemoveProperty implements TermUpdateRequest {
    @JsonProperty("property")
    private final String property;

    /**
     * Creates the removeProperty update.
     *
     * @param property The property name to remove.
     */
    @JsonCreator
    public RemoveProperty(@JsonProperty(value = "property", required = true) String property) {
      this.property = property;
    }

    @Override
    public TermChange toChange() {
      return TermChange.removeProperty(property);
    }
  }
}
