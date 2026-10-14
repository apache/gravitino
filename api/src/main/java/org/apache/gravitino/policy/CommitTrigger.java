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
package org.apache.gravitino.policy;

/**
 * Trigger that evaluates the policy after an Iceberg commit.
 *
 * <p>Presence of this object means commit-triggered evaluation is enabled. There is no nested
 * {@code onCommit} flag.
 */
public final class CommitTrigger implements Trigger {

  /** Shared instance for callers that do not need a distinct object identity. */
  public static final CommitTrigger INSTANCE = new CommitTrigger();

  /** Creates a commit trigger. */
  public CommitTrigger() {}

  @Override
  public void validate() {
    // Presence alone enables the trigger; nothing else to validate.
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CommitTrigger;
  }

  @Override
  public int hashCode() {
    return CommitTrigger.class.hashCode();
  }

  @Override
  public String toString() {
    return "CommitTrigger{}";
  }
}
