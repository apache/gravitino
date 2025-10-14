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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.apache.gravitino.dto.job.ShellTemplateUpdateDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateUpdatesRequest {

  @Test
  public void testSerDeRequest() throws JsonProcessingException {
    JobTemplateUpdateRequest renameReq =
        new JobTemplateUpdateRequest.RenameJobTemplateRequest("new_template_name");
    JobTemplateUpdateRequest updateCommentReq =
        new JobTemplateUpdateRequest.UpdateJobTemplateCommentRequest("Updated comment");
    JobTemplateUpdateRequest updateContentReq =
        new JobTemplateUpdateRequest.UpdateJobTemplateContentRequest(
            ShellTemplateUpdateDTO.builder().withNewExecutable("/bin/bash").build());

    JobTemplateUpdatesRequest request =
        new JobTemplateUpdatesRequest(
            ImmutableList.of(renameReq, updateCommentReq, updateContentReq));

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    JobTemplateUpdatesRequest deserializedRequest =
        JsonUtils.objectMapper().readValue(json, JobTemplateUpdatesRequest.class);

    Assertions.assertEquals(request, deserializedRequest);
    Assertions.assertTrue(deserializedRequest.getUpdates().contains(renameReq));
    Assertions.assertTrue(deserializedRequest.getUpdates().contains(updateCommentReq));
    Assertions.assertTrue(deserializedRequest.getUpdates().contains(updateContentReq));
  }
}
