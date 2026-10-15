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

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Base for built-in Iceberg maintenance policy contents that carry optional execution peers.
 *
 * <p>Peers are supplied at construction as an immutable {@link PolicyExecutionSpec}. Subclasses
 * keep strategy fields separate and pass the spec through {@link
 * #AbstractExecutablePolicyContent(PolicyExecutionSpec)}. Persisted JSON keeps peer keys flat via
 * {@link JsonUnwrapped}.
 */
public abstract class AbstractExecutablePolicyContent implements ExecutablePolicyContent {

  @JsonUnwrapped private final PolicyExecutionSpec executionSpec;

  /**
   * Creates content with the given execution peers.
   *
   * @param executionSpec immutable peers; {@code null} is treated as {@link
   *     PolicyExecutionSpec#empty()}
   */
  protected AbstractExecutablePolicyContent(@Nullable PolicyExecutionSpec executionSpec) {
    this.executionSpec = executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
  }

  /** Creates content with unset execution peers. */
  protected AbstractExecutablePolicyContent() {
    this(PolicyExecutionSpec.empty());
  }

  @Override
  public PolicyExecutionSpec executionSpec() {
    return executionSpec;
  }

  /**
   * Validates universal {@link PolicyContent} rules plus shared execution-peer checks.
   *
   * <p>Does not enforce type-specific maintenance contracts (for example orphan cleanup forbidding
   * {@code commitTrigger}, or cron parsing). Those belong on concrete subclasses or the server
   * create/alter path with the real {@link Policy.BuiltInType}.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    ExecutablePolicyContent.super.validate();
    executionSpec.validate();
  }

  /**
   * Whether execution fields match.
   *
   * @param that other content
   * @return true when execution fields are equal
   */
  protected boolean executionFieldsEqual(AbstractExecutablePolicyContent that) {
    return Objects.equals(executionSpec, that.executionSpec);
  }

  /**
   * @return hash of execution fields
   */
  protected int executionFieldsHashCode() {
    return Objects.hashCode(executionSpec);
  }

  /**
   * Appends execution fields to a subclass {@code toString} body.
   *
   * <p>Option map <em>values</em> are never rendered (keys only) so accidental credentials are not
   * exposed through logs or exception messages.
   *
   * <p>Callers that already wrote subclass fields should append {@code ", "} before invoking this
   * method.
   *
   * @param sb builder already containing an opening brace and any subclass fields
   */
  protected void appendExecutionFields(StringBuilder sb) {
    sb.append("timeTrigger='")
        .append(executionSpec.timeTrigger())
        .append('\'')
        .append(", commitTrigger=")
        .append(executionSpec.commitTrigger())
        .append(", minIntervalMs=")
        .append(executionSpec.minIntervalMs())
        .append(", jobOptionsKeys=")
        .append(executionSpec.jobOptions().keySet())
        .append(", updateOptionsKeys=")
        .append(executionSpec.updateOptions().keySet());
  }
}
