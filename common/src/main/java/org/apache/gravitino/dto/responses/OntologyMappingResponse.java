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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.gravitino.dto.ontology.OntologyMappingDTO;

/** REST response containing the current mapping. */
public final class OntologyMappingResponse extends BaseResponse {
  @JsonProperty("ontologyMapping")
  private final OntologyMappingDTO ontologyMapping;

  /** Creates a response for deserialization. */
  public OntologyMappingResponse() {
    this.ontologyMapping = null;
  }

  /**
   * Creates a successful response.
   *
   * @param item The current mapping.
   */
  public OntologyMappingResponse(OntologyMappingDTO item) {
    super(0);
    this.ontologyMapping = item;
  }

  /**
   * @return The current mapping.
   */
  public OntologyMappingDTO ontologyMapping() {
    return ontologyMapping;
  }

  /** {@inheritDoc} */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(ontologyMapping != null, "Missing ontologyMapping in response");
  }
}
