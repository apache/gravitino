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
package org.apache.gravitino.dto.job;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.gravitino.job.JobTemplateChange;

/** DTO for updating an HTTP Job Template. */
@Getter
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(setterPrefix = "with")
@ToString(callSuper = true)
public class HttpTemplateUpdateDTO extends TemplateUpdateDTO {

  @Nullable
  @JsonProperty("newUrl")
  private String newUrl;

  @Nullable
  @JsonProperty("newHeaders")
  private Map<String, String> newHeaders;

  @Nullable
  @JsonProperty("newBody")
  private String newBody;

  @Nullable
  @JsonProperty("newQueryParams")
  private List<String> newQueryParams;

  /**
   * The default constructor for Jackson. This constructor is required for deserialization of the
   * DTO.
   */
  private HttpTemplateUpdateDTO() {
    // Default constructor for Jackson
    super();
  }

  @Override
  public JobTemplateChange.TemplateUpdate toTemplateUpdate() {
    return JobTemplateChange.HttpTemplateUpdate.builder()
        .withNewExecutable(getNewExecutable())
        .withNewArguments(getNewArguments())
        .withNewEnvironments(getNewEnvironments())
        .withNewCustomFields(getNewCustomFields())
        .withNewUrl(newUrl)
        .withNewHeaders(newHeaders)
        .withNewBody(newBody)
        .withNewQueryParams(newQueryParams)
        .build();
  }
}
