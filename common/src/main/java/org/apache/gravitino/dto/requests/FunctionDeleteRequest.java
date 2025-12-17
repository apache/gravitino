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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.function.FunctionSignatureDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Optional delete request for functions to specify a signature. */
@Getter
@EqualsAndHashCode
@ToString
public class FunctionDeleteRequest implements RESTRequest {

  @JsonProperty("signature")
  private FunctionSignatureDTO signature;

  private FunctionDeleteRequest() {}

  /**
   * Creates a delete request targeting a specific signature.
   *
   * @param signature Optional signature to disambiguate overloaded functions.
   */
  public FunctionDeleteRequest(FunctionSignatureDTO signature) {
    this.signature = signature;
  }

  /** {@inheritDoc} */
  @Override
  public void validate() throws IllegalArgumentException {
    if (signature != null) {
      signature.validate();
    }
  }
}
