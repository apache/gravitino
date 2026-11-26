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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents an atomic request to alter a Semantic Model. */
@Getter
@EqualsAndHashCode
@ToString
public class SemanticModelUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<SemanticModelUpdateRequest> updates;

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("expectedRevision")
  @JsonDeserialize(using = ExpectedRevisionDeserializer.class)
  private final Integer expectedRevision;

  /** Default constructor for Jackson deserialization. */
  public SemanticModelUpdatesRequest() {
    this(null);
  }

  /**
   * Creates an atomic Semantic Model updates request.
   *
   * @param updates The non-empty list of updates to apply.
   */
  public SemanticModelUpdatesRequest(List<SemanticModelUpdateRequest> updates) {
    this(updates, null);
  }

  /**
   * Creates an alteration request with an optional revision precondition.
   *
   * @param updates The non-empty list of updates to apply.
   * @param expectedRevision The expected latest revision, or null if omitted.
   */
  public SemanticModelUpdatesRequest(
      List<SemanticModelUpdateRequest> updates, @Nullable Integer expectedRevision) {
    this.updates = updates;
    this.expectedRevision = expectedRevision;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(updates != null, "updates must not be null");
    Preconditions.checkArgument(!updates.isEmpty(), "updates must not be empty");
    Preconditions.checkArgument(
        expectedRevision == null || expectedRevision > 0, "expectedRevision must be positive");
    for (SemanticModelUpdateRequest update : updates) {
      Preconditions.checkArgument(update != null, "update must not be null");
      Preconditions.checkArgument(
          !(update instanceof SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest)
              || expectedRevision != null,
          "expectedRevision is required when replacing the definition");
      Preconditions.checkArgument(
          !(update instanceof SemanticModelUpdateRequest.RenameSemanticModelRequest)
              || updates.size() == 1,
          "Rename must be the only Semantic Model change");
      update.validate();
    }
  }

  private static class ExpectedRevisionDeserializer extends JsonDeserializer<Integer> {
    @Override
    public Integer deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (!parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
        return context.reportInputMismatch(
            Integer.class, "expectedRevision must be a 32-bit integer");
      }
      return parser.getIntValue();
    }
  }
}
