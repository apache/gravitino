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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.tag.EffectiveTagResolver;

/** Resolves tag assignments and policy bindings once per target object and request. */
public final class TagPolicyResolver {
  private final TagPolicyReader reader;

  /**
   * Creates a resolver with a storage-backed or preloaded reader.
   *
   * @param reader the source of direct assignments and policy bindings
   */
  public TagPolicyResolver(TagPolicyReader reader) {
    this.reader = Objects.requireNonNull(reader, "reader must not be null");
  }

  /**
   * Resolves enabled access policies and their effective tag assignments at the target.
   *
   * @param metalake the containing metalake
   * @param target the original object whose access is being checked
   * @param context the context shared by checks and list workers of this request
   * @return immutable bindings in tag-name and policy-ID order; selectors have not been evaluated
   * @throws RuntimeException if any tag or policy read fails
   */
  public List<ResolvedTagPolicy> resolve(
      String metalake, MetadataObject target, AuthorizationRequestContext context) {
    Objects.requireNonNull(metalake, "metalake must not be null");
    Objects.requireNonNull(target, "target must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return context.computeTagPoliciesIfAbsent(metalake, target, () -> resolve(metalake, target));
  }

  private List<ResolvedTagPolicy> resolve(String metalake, MetadataObject target) {
    TagEntity[] tags =
        EffectiveTagResolver.resolve(target, object -> reader.loadTags(metalake, object));
    if (tags.length == 0) {
      return Collections.emptyList();
    }
    return reader.loadPolicyBindings(metalake, Arrays.asList(tags)).stream()
        .filter(binding -> binding.policy().enabled())
        .filter(binding -> binding.policy().policyType() == Policy.BuiltInType.ACCESS_CONTROL)
        .sorted(
            Comparator.comparing((ResolvedTagPolicy binding) -> binding.tag().name())
                .thenComparing(binding -> binding.policy().id()))
        .collect(Collectors.toList());
  }
}
