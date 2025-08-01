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

import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/**
 * Interface for supporting policy changes. This interface will be used to provide policy
 * modification operations for each policy.
 */
@Evolving
public interface PolicyChange {

  /**
   * Creates a new policy change to rename the policy.
   *
   * @param newName The new name of the policy.
   * @return The policy change.
   */
  static PolicyChange rename(String newName) {
    return new RenamePolicy(newName);
  }

  /**
   * Creates a new policy change to update the policy comment.
   *
   * @param newComment The new comment for the policy.
   * @return The policy change.
   */
  static PolicyChange updateComment(String newComment) {
    return new UpdatePolicyComment(newComment);
  }

  /**
   * Creates a new policy change to update the content of the policy.
   *
   * @param policyType The type of the policy, used for validation.
   * @param content The new content for the policy.
   * @return The policy change.
   */
  static PolicyChange updateContent(String policyType, PolicyContent content) {
    return new UpdateContent(policyType, content);
  }

  /** A policy change to rename the policy. */
  final class RenamePolicy implements PolicyChange {
    private final String newName;

    private RenamePolicy(String newName) {
      this.newName = newName;
    }

    /**
     * Get the new name of the policy.
     *
     * @return The new name of the policy.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Get the type of the policy change.
     *
     * @return The type of the policy change.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      RenamePolicy that = (RenamePolicy) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Get the hash code of the policy change.
     *
     * @return The hash code of the policy change.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(newName);
    }

    /**
     * Get the string representation of the policy change.
     *
     * @return The string representation of the policy change.
     */
    @Override
    public String toString() {
      return "RENAME POLICY " + newName;
    }
  }

  /** Creates a new policy change to update the comment of the policy. */
  final class UpdatePolicyComment implements PolicyChange {
    private final String newComment;

    private UpdatePolicyComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Get the new comment of the policy.
     *
     * @return The new comment of the policy.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Get the type of the policy change.
     *
     * @return The type of the policy change.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      UpdatePolicyComment that = (UpdatePolicyComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Get the hash code of the policy change.
     *
     * @return The hash code of the policy change.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(newComment);
    }

    /**
     * Get the string representation of the policy change.
     *
     * @return The string representation of the policy change.
     */
    @Override
    public String toString() {
      return "UPDATE POLICY COMMENT " + newComment;
    }
  }

  /** A policy change to update the content of the policy. */
  final class UpdateContent implements PolicyChange {
    private final String policyType;
    private final PolicyContent content;

    private UpdateContent(String policyType, PolicyContent content) {
      this.policyType = policyType;
      this.content = content;
    }

    /**
     * Get the type of the policy.
     *
     * @return Get the type of the policy.
     */
    public String getPolicyType() {
      return policyType;
    }

    /**
     * Get the content of the policy change.
     *
     * @return The content of the policy change.
     */
    public PolicyContent getContent() {
      return content;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof UpdateContent)) return false;
      UpdateContent that = (UpdateContent) o;
      return Objects.equals(policyType, that.policyType) && Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
      return Objects.hash(policyType, content);
    }

    @Override
    public String toString() {
      return "UPDATE POLICY CONTENT " + "policyType=" + policyType + ", content=" + content;
    }
  }
}
