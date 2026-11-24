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

package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Result of a glossary write or history read. Version numbers are scoped to the stable term ID. */
@Evolving
public final class TermPublication {
  /** Lifecycle of this exact version. */
  public enum State {
    /** Published at least once. The published version ID identifies the currently live snapshot. */
    PUBLISHED,
    /** Saved privately without submission. */
    DRAFT,
    /** Awaiting a publication decision. */
    SUBMITTED,
    /** A decision declined this exact candidate. */
    REJECTED,
    /** A newer candidate replaced this one. */
    SUPERSEDED
  }

  private final long termId;
  private final long versionId;
  private final State state;
  @Nullable private final Long publishedVersionId;
  @Nullable private final Long candidateVersionId;

  /**
   * Describes one version and the current publication state of its term.
   *
   * @param termId stable term ID shared by all its versions
   * @param versionId positive version number of the returned snapshot
   * @param state lifecycle state of the returned version
   * @param publishedVersionId current published version, or null before first publication
   * @param candidateVersionId current candidate version, or null if no candidate is visible
   */
  public TermPublication(
      long termId,
      long versionId,
      State state,
      @Nullable Long publishedVersionId,
      @Nullable Long candidateVersionId) {
    Preconditions.checkArgument(versionId >= 1, "versionId must be positive");
    Preconditions.checkArgument(state != null, "state cannot be null");
    this.termId = termId;
    this.versionId = versionId;
    this.state = state;
    this.publishedVersionId = publishedVersionId;
    this.candidateVersionId = candidateVersionId;
  }

  /**
   * @return the stable term ID retained across renames and publication
   */
  public long termId() {
    return termId;
  }

  /**
   * @return the version number of the returned snapshot
   */
  public long versionId() {
    return versionId;
  }

  /**
   * @return the lifecycle state of the returned version
   */
  public State state() {
    return state;
  }

  /**
   * @return the current published version, or null before first publication
   */
  @Nullable
  public Long publishedVersionId() {
    return publishedVersionId;
  }

  /**
   * @return the current candidate version, or null if no candidate is visible
   */
  @Nullable
  public Long candidateVersionId() {
    return candidateVersionId;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TermPublication)) {
      return false;
    }
    TermPublication that = (TermPublication) o;
    return termId == that.termId
        && versionId == that.versionId
        && state == that.state
        && Objects.equals(publishedVersionId, that.publishedVersionId)
        && Objects.equals(candidateVersionId, that.candidateVersionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(termId, versionId, state, publishedVersionId, candidateVersionId);
  }

  @Override
  public String toString() {
    return "TermPublication{termId="
        + termId
        + ", versionId="
        + versionId
        + ", state="
        + state
        + ", publishedVersionId="
        + publishedVersionId
        + ", candidateVersionId="
        + candidateVersionId
        + "}";
  }
}
