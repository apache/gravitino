/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.gravitino.file.ClientType;
import org.apache.gravitino.file.FilesetDataOperation;
import org.apache.gravitino.rest.RESTRequest;

/** Request to get a fileset context. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Builder
@ToString
public class GetFilesetContextRequest implements RESTRequest {
  @JsonProperty("subPath")
  private String subPath;

  @JsonProperty("operation")
  private FilesetDataOperation operation;

  @JsonProperty("clientType")
  private ClientType clientType;

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(subPath != null, "\"subPath\" field cannot be null");
    Preconditions.checkArgument(
        operation != null, "\"operation\" field is required and cannot be null");
    Preconditions.checkArgument(
        clientType != null, "\"clientType\" field is required and cannot be null");
  }
}
