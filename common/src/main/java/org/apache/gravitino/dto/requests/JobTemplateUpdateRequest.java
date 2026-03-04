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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.job.TemplateUpdateDTO;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.rest.RESTRequest;

/**
 * Represents a request to update a job template. This can include renaming the template, updating
 * its comment, or changing its content.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = JobTemplateUpdateRequest.RenameJobTemplateRequest.class,
      name = "rename"),
  @JsonSubTypes.Type(
      value = JobTemplateUpdateRequest.UpdateJobTemplateCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = JobTemplateUpdateRequest.UpdateJobTemplateContentRequest.class,
      name = "updateTemplate")
})
public interface JobTemplateUpdateRequest extends RESTRequest {

  /**
   * Get the job template change that is requested.
   *
   * @return the job template change
   */
  JobTemplateChange jobTemplateChange();

  /** The request to rename a job template. */
  @EqualsAndHashCode
  @ToString
  class RenameJobTemplateRequest implements JobTemplateUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Constructor for RenameJobTemplateRequest.
     *
     * @param newName the new name for the job template
     */
    public RenameJobTemplateRequest(String newName) {
      this.newName = newName;
    }

    /** Default constructor for Jackson. */
    private RenameJobTemplateRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" is required and cannot be empty");
    }

    @Override
    public JobTemplateChange jobTemplateChange() {
      return JobTemplateChange.rename(newName);
    }
  }

  /** The request to update the comment of a job template. */
  @EqualsAndHashCode
  @ToString
  class UpdateJobTemplateCommentRequest implements JobTemplateUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateJobTemplateCommentRequest.
     *
     * @param newComment the new comment for the job template
     */
    public UpdateJobTemplateCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** Default constructor for Jackson. */
    private UpdateJobTemplateCommentRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      // No specific validation needed for comment, it can be null or empty
    }

    @Override
    public JobTemplateChange jobTemplateChange() {
      return JobTemplateChange.updateComment(newComment);
    }
  }

  /** The request to update the content of a job template. */
  @EqualsAndHashCode
  @ToString
  class UpdateJobTemplateContentRequest implements JobTemplateUpdateRequest {

    @Getter
    @JsonProperty("newTemplate")
    private final TemplateUpdateDTO newTemplate;

    /**
     * Constructor for UpdateJobTemplateContentRequest.
     *
     * @param newTemplate the new template content for the job template
     */
    public UpdateJobTemplateContentRequest(TemplateUpdateDTO newTemplate) {
      this.newTemplate = newTemplate;
    }

    /** Default constructor for Jackson. */
    private UpdateJobTemplateContentRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newTemplate != null, "\"newTemplate\" is required and cannot be null");
    }

    @Override
    public JobTemplateChange jobTemplateChange() {
      return JobTemplateChange.updateTemplate(newTemplate.toTemplateUpdate());
    }
  }
}
