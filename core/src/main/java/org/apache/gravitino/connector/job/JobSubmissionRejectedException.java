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

package org.apache.gravitino.connector.job;

/**
 * Signals that a job submission failed without leaving any execution in the job runner.
 *
 * <p>Only throw this exception when no execution was created, or its removal is confirmed.
 * Transport failures with an unknown outcome must use another exception so Gravitino retains the
 * registration for recovery.
 */
public class JobSubmissionRejectedException extends RuntimeException {
  /**
   * Creates a confirmed rejection.
   *
   * @param message the reason the job was not submitted
   */
  public JobSubmissionRejectedException(String message) {
    super(message);
  }

  /**
   * Creates a confirmed rejection with its original failure.
   *
   * @param message the reason the job was not submitted
   * @param cause the original failure
   */
  public JobSubmissionRejectedException(String message, Throwable cause) {
    super(message, cause);
  }
}
