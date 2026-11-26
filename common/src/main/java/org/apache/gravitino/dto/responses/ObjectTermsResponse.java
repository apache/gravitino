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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.glossary.TermDTO;

/** A metadata object's visible direct terms and requested child terms. */
public class ObjectTermsResponse extends TermListResponse {
  /** Terms of one table column or semantic-model metric. */
  public static final class Child {
    private final String name;
    private final List<TermDTO> terms;

    /**
     * Creates a named child with its visible terms.
     *
     * @param name The column or metric name.
     * @param terms The non-empty list of visible terms bound to this child.
     */
    @JsonCreator
    public Child(@JsonProperty("name") String name, @JsonProperty("terms") List<TermDTO> terms) {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "Child name must not be blank");
      Preconditions.checkArgument(
          terms != null && !terms.isEmpty(), "A child must have visible terms");
      this.name = name;
      this.terms = ImmutableList.copyOf(terms);
    }

    /**
     * @return The column or metric name.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The visible terms bound to this child.
     */
    public List<TermDTO> getTerms() {
      return terms;
    }
  }

  @JsonProperty("children")
  private List<Child> children;

  /** Creates an empty response for deserialization. */
  public ObjectTermsResponse() {}

  /**
   * Creates the direct and child term response.
   *
   * @param terms The visible terms bound directly to the object.
   * @param children The requested children with visible matching terms.
   */
  public ObjectTermsResponse(TermDTO[] terms, List<Child> children) {
    super(terms);
    this.children = ImmutableList.copyOf(children);
  }

  /**
   * @return The children with visible matching terms, or {@code null} if unset.
   */
  public List<Child> getChildren() {
    return children == null ? null : Collections.unmodifiableList(children);
  }

  /** Validates the response envelope and its result collections. */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(children != null, "children are required");
    for (Child child : children) {
      Preconditions.checkArgument(child != null, "children must not contain null");
    }
  }
}
