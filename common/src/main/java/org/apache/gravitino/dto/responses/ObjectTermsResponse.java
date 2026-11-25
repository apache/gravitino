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
import java.util.List;
import org.apache.gravitino.dto.glossary.TermDTO;

/** A metadata object's visible direct terms and requested child terms. */
public class ObjectTermsResponse extends BaseResponse {
  /** Terms of one table column or semantic-model metric. */
  public static final class Child {
    private final String name;
    private final List<TermDTO> terms;

    /** Creates a named child with its visible terms. */
    @JsonCreator
    public Child(@JsonProperty("name") String name, @JsonProperty("terms") List<TermDTO> terms) {
      this.name = name;
      this.terms = ImmutableList.copyOf(terms);
    }
    /** Returns the column or metric name. */
    public String getName() {
      return name;
    }
    /** Returns the visible terms bound to this child. */
    public List<TermDTO> getTerms() {
      return terms;
    }
  }

  @JsonProperty("terms")
  private TermDTO[] terms;

  @JsonProperty("children")
  private List<Child> children;

  /** Creates an empty response for deserialization. */
  public ObjectTermsResponse() {}

  /** Creates the direct and child term response. */
  public ObjectTermsResponse(TermDTO[] terms, List<Child> children) {
    super(0);
    this.terms = terms.clone();
    this.children = ImmutableList.copyOf(children);
  }
  /** Returns terms bound directly to the requested object. */
  public TermDTO[] getTerms() {
    return terms == null ? null : terms.clone();
  }
  /** Returns only children with visible matching terms. */
  public List<Child> getChildren() {
    return children;
  }
  /** Validates the response envelope and its result collections. */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(
        terms != null && children != null, "terms and children are required");
  }
}
