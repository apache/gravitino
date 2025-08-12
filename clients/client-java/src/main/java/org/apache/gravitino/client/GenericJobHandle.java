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
package org.apache.gravitino.client;

import org.apache.gravitino.dto.job.JobDTO;
import org.apache.gravitino.job.JobHandle;

/** Represents a generic job handle. */
public class GenericJobHandle implements JobHandle {

  private final JobDTO jobDTO;

  GenericJobHandle(JobDTO jobDTO) {
    this.jobDTO = jobDTO;
  }

  @Override
  public String jobTemplateName() {
    return jobDTO.jobTemplateName();
  }

  @Override
  public String jobId() {
    return jobDTO.jobId();
  }

  @Override
  public Status jobStatus() {
    return jobDTO.status();
  }
}
