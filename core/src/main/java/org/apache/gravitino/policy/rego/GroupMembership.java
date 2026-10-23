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
package org.apache.gravitino.policy.rego;

import com.google.common.base.Preconditions;
import java.util.Objects;

/** A request-context group-membership predicate. */
final class GroupMembership implements CanonicalExpression {
  private final String group;

  GroupMembership(String group) {
    this.group = group;
  }

  String group() {
    return group;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(group != null && !group.isEmpty(), "group name cannot be empty");
    ExpressionValidator.validateUnicodeScalars(group, "group name");
  }

  @Override
  public int depth() {
    return 1;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof GroupMembership)) {
      return false;
    }
    GroupMembership that = (GroupMembership) other;
    return Objects.equals(group, that.group);
  }

  @Override
  public int hashCode() {
    return Objects.hash(group);
  }

  @Override
  public String toString() {
    return "GroupMembership{" + "group='" + group + '\'' + '}';
  }
}
