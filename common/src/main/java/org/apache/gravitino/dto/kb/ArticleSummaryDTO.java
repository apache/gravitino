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
package org.apache.gravitino.dto.kb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleSummary;

/** The fields returned for one visible Knowledge Base Article in a list. */
@Getter
public class ArticleSummaryDTO {
  private final String name;
  private final String title;

  @JsonSerialize(using = ToStringSerializer.class)
  private final ArticleStatus status;

  private final List<String> domains;

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @Nullable
  private final OwnerDTO owner;

  private final List<ArticleTermDTO> terms;
  private final boolean termsHasMore;

  @JsonProperty("audit_info")
  private final AuditDTO auditInfo;

  @JsonCreator
  private ArticleSummaryDTO(
      @JsonProperty("name") String name,
      @JsonProperty("title") String title,
      @JsonProperty("status") ArticleStatus status,
      @JsonProperty("domains") List<String> domains,
      @JsonProperty("owner") @Nullable OwnerDTO owner,
      @JsonProperty("terms") List<ArticleTermDTO> terms,
      @JsonProperty("termsHasMore") boolean termsHasMore,
      @JsonProperty("audit_info") AuditDTO auditInfo) {
    this.name = name;
    this.title = title;
    this.status = status;
    this.domains = ImmutableList.copyOf(domains);
    this.owner = owner;
    this.terms = ImmutableList.copyOf(terms);
    this.termsHasMore = termsHasMore;
    this.auditInfo = auditInfo;
  }

  /**
   * Converts a visible Article summary to its wire representation.
   *
   * @param summary the Article summary
   * @return the summary DTO
   */
  public static ArticleSummaryDTO from(ArticleSummary summary) {
    Owner owner = summary.owner();
    return new ArticleSummaryDTO(
        summary.name(),
        summary.title(),
        summary.status(),
        Arrays.asList(summary.domains()),
        owner == null ? null : DTOConverters.toDTO(owner),
        Arrays.stream(summary.terms())
            .map(ArticleTermDTO::from)
            .collect(ImmutableList.toImmutableList()),
        summary.termsHasMore(),
        DTOConverters.toDTO(summary.auditInfo()));
  }
}
