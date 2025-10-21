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
package org.apache.gravitino.lance.service.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LanceListNamespacesResponse {

  @JsonProperty("id")
  private final String id;

  @JsonProperty("delimiter")
  private final String delimiter;

  @JsonProperty("namespaces")
  private final List<String> namespaces;

  @JsonProperty("next_page_token")
  private final String nextPageToken;

  public LanceListNamespacesResponse(
      String id, String delimiter, List<String> namespaces, String nextPageToken) {
    this.id = id;
    this.delimiter = delimiter;
    this.namespaces = List.copyOf(namespaces);
    this.nextPageToken = nextPageToken;
  }

  public String getId() {
    return id;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public List<String> getNamespaces() {
    return namespaces;
  }

  public String getNextPageToken() {
    return nextPageToken;
  }
}
