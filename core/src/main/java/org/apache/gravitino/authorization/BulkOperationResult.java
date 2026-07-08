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
package org.apache.gravitino.authorization;

/** Represents the result of a bulk operation. */
public class BulkOperationResult {

  private final String[] succeeded;
  private final Failure[] failed;

  /**
   * Creates a new BulkOperationResult instance.
   *
   * @param succeeded The names that were processed successfully.
   * @param failed The failures for individual names.
   */
  public BulkOperationResult(String[] succeeded, Failure[] failed) {
    this.succeeded = succeeded;
    this.failed = failed;
  }

  /**
   * Gets the names that were processed successfully.
   *
   * @return The names that were processed successfully.
   */
  public String[] succeeded() {
    return succeeded;
  }

  /**
   * Gets the failures for individual names.
   *
   * @return The failures for individual names.
   */
  public Failure[] failed() {
    return failed;
  }

  /** Represents a failure for a single bulk operation item. */
  public static class Failure {
    private final String name;
    private final String reason;

    /**
     * Creates a new Failure instance.
     *
     * @param name The failed name.
     * @param reason The failure reason.
     */
    public Failure(String name, String reason) {
      this.name = name;
      this.reason = reason;
    }

    /**
     * Gets the failed name.
     *
     * @return The failed name.
     */
    public String name() {
      return name;
    }

    /**
     * Gets the failure reason.
     *
     * @return The failure reason.
     */
    public String reason() {
      return reason;
    }
  }
}
