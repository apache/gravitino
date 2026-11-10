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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.pagination.Page;
import org.apache.gravitino.pagination.PageRequest;

/**
 * Interface for policy operations on a metadata object.
 *
 * <p>The lookup methods return policies derived from the object's effective tag assignments and
 * matching policy-to-tag associations.
 */
@Evolving
public interface SupportsPolicies {
  /**
   * @return The names of the policies that apply to the metadata object.
   */
  String[] listPolicies();

  /**
   * List the visible policy names and whether authorization filtering omitted other policies.
   *
   * <p>The default implementation preserves compatibility with existing implementations but cannot
   * detect omitted policies. Implementations that support authorization filtering should override
   * this method.
   *
   * @return The visible policy names and authorization-filtering status.
   */
  default PolicyList<String> listPoliciesWithVisibility() {
    return new PolicyList<>(listPolicies(), false);
  }

  /**
   * @return The policies that apply to the metadata object.
   */
  Policy[] listPolicyInfos();

  /**
   * List the visible policies with details and whether authorization filtering omitted other
   * policies.
   *
   * <p>The default implementation preserves compatibility with existing implementations but cannot
   * detect omitted policies. Implementations that support authorization filtering should override
   * this method.
   *
   * @return The visible policies and authorization-filtering status.
   */
  default PolicyList<Policy> listPolicyInfosWithVisibility() {
    return new PolicyList<>(listPolicyInfos(), false);
  }

  /**
   * List one page of the names of the policies associated with the object, in ascending order.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param pageRequest The page to read.
   * @return The page.
   */
  default Page<String> listPolicies(PageRequest pageRequest) {
    throw new UnsupportedOperationException("Paged listing is not supported");
  }

  /**
   * List one page of the policies associated with the object, in ascending order of name.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param pageRequest The page to read.
   * @return The page.
   */
  default Page<Policy> listPolicyInfos(PageRequest pageRequest) {
    throw new UnsupportedOperationException("Paged listing is not supported");
  }
}
