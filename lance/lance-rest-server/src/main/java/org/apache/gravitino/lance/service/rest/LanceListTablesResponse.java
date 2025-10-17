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
public class LanceListTablesResponse {

  @JsonProperty("id")
  private final String namespaceId;

  @JsonProperty("delimiter")
  private final String delimiter;

  @JsonProperty("tables")
  private final List<String> tables;

  @JsonProperty("next_page_token")
  private final String nextPageToken;

  public LanceListTablesResponse(
      String namespaceId, String delimiter, List<String> tables, String nextPageToken) {
    this.namespaceId = namespaceId;
    this.delimiter = delimiter;
    this.tables = List.copyOf(tables);
    this.nextPageToken = nextPageToken;
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public List<String> getTables() {
    return tables;
  }

  public String getNextPageToken() {
    return nextPageToken;
  }
}
