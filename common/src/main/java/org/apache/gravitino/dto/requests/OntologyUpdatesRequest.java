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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDeserializer;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.rest.RESTRequest;

/** An ordered batch of ontology updates. */
public final class OntologyUpdatesRequest implements RESTRequest {
  @JsonProperty("updates")
  private final List<Update> updates;

  /**
   * Creates a request containing a batch of changes.
   *
   * @param updates The ordered changes to apply.
   */
  @JsonCreator
  public OntologyUpdatesRequest(
      @JsonProperty(value = "updates", required = true) List<Update> updates) {
    Preconditions.checkArgument(
        updates != null && updates.stream().noneMatch(Objects::isNull), "updates are required");
    this.updates = ImmutableList.copyOf(updates);
  }

  /**
   * Converts native ontology changes to the wire format.
   *
   * @param changes The ontology changes.
   * @return The corresponding request.
   */
  public static OntologyUpdatesRequest fromOntologyChanges(OntologyChange... changes) {
    return new OntologyUpdatesRequest(
        Arrays.stream(changes)
            .map(Update::fromOntologyChange)
            .collect(ImmutableList.toImmutableList()));
  }

  /**
   * @return The ordered ontology changes.
   */
  public OntologyChange[] ontologyChanges() {
    return updates.stream().map(Update::ontologyChange).toArray(OntologyChange[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public void validate() {
    Preconditions.checkArgument(!updates.isEmpty(), "At least one update is required");
  }

  /** One operation and its associated fields. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class Update {
    private static final String RENAME = "rename";
    private static final String UPDATE_COMMENT = "updateComment";
    private static final String SET_PROPERTY = "setProperty";
    private static final String REMOVE_PROPERTY = "removeProperty";
    private static final String REPLACE_DEFINITION = "replaceDefinition";

    @JsonProperty("@type")
    private final String type;

    @JsonProperty("newName")
    @Nullable
    private final String newName;

    @JsonProperty("newComment")
    @Nullable
    private final String newComment;

    @JsonProperty("property")
    @Nullable
    private final String property;

    @JsonProperty("value")
    @Nullable
    private final String value;

    @JsonProperty("definition")
    @Nullable
    private final Map<String, Object> definition;

    /**
     * Creates an update from its JSON representation.
     *
     * @param type The operation type.
     * @param newName The rename target.
     * @param newComment The replacement comment or null to remove it.
     * @param property The property to set or remove.
     * @param value The replacement property value.
     * @param definition The replacement definition.
     */
    @JsonCreator
    public Update(
        @JsonProperty(value = "@type", required = true) String type,
        @JsonProperty("newName") @Nullable String newName,
        @JsonProperty("newComment") @Nullable String newComment,
        @JsonProperty("property") @Nullable String property,
        @JsonProperty("value") @Nullable String value,
        @JsonProperty("definition")
            @JsonDeserialize(using = OntologyDefinitionDeserializer.class)
            @Nullable
            Map<String, Object> definition) {
      this.type = required(type, "@type");
      this.newName = newName;
      this.newComment = newComment;
      this.property = property;
      this.value = value;
      this.definition = definition;
    }

    private static Update fromOntologyChange(OntologyChange change) {
      if (change instanceof OntologyChange.RenameOntology) {
        return new Update(
            RENAME, ((OntologyChange.RenameOntology) change).getNewName(), null, null, null, null);
      } else if (change instanceof OntologyChange.UpdateComment) {
        return new Update(
            UPDATE_COMMENT,
            null,
            ((OntologyChange.UpdateComment) change).getNewComment(),
            null,
            null,
            null);
      } else if (change instanceof OntologyChange.SetProperty) {
        OntologyChange.SetProperty set = (OntologyChange.SetProperty) change;
        return new Update(SET_PROPERTY, null, null, set.getProperty(), set.getValue(), null);
      } else if (change instanceof OntologyChange.RemoveProperty) {
        return new Update(
            REMOVE_PROPERTY,
            null,
            null,
            ((OntologyChange.RemoveProperty) change).getProperty(),
            null,
            null);
      } else if (change instanceof OntologyChange.ReplaceDefinition) {
        return new Update(
            REPLACE_DEFINITION,
            null,
            null,
            null,
            null,
            ((OntologyChange.ReplaceDefinition) change).getDefinition().fields());
      }
      throw new IllegalArgumentException("Unsupported ontology change: " + change);
    }

    private OntologyChange ontologyChange() {
      switch (type) {
        case RENAME:
          return OntologyChange.rename(required(newName, "newName"));
        case UPDATE_COMMENT:
          return OntologyChange.updateComment(newComment);
        case SET_PROPERTY:
          return OntologyChange.setProperty(
              required(property, "property"), required(value, "value"));
        case REMOVE_PROPERTY:
          return OntologyChange.removeProperty(required(property, "property"));
        case REPLACE_DEFINITION:
          return OntologyChange.replaceDefinition(
              OntologyDefinition.of(required(definition, "definition")));
        default:
          throw new IllegalArgumentException("Unknown update type: " + type);
      }
    }

    private static <T> T required(@Nullable T value, String field) {
      Preconditions.checkArgument(value != null, "%s is required", field);
      return value;
    }
  }
}
