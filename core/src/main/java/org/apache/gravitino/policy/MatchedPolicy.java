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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * An enabled policy matched for a metadata object, together with the metadata-object sources that
 * contributed matching effective tags.
 *
 * <p>A policy may match through multiple tags assigned at different levels; {@link #sources()}
 * preserves those contributing objects in encounter order.
 */
public final class MatchedPolicy {

  private final PolicyEntity policy;
  private final List<MetadataObject> sources;

  /**
   * @param policy matched enabled policy (inherited flag already applied)
   * @param sources contributing metadata objects, nearest-first encounter order
   */
  public MatchedPolicy(PolicyEntity policy, List<MetadataObject> sources) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.sources =
        Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(sources, "sources")));
  }

  /**
   * @return the matched policy
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return metadata objects whose effective tag assignments caused this policy to match
   */
  public List<MetadataObject> sources() {
    return sources;
  }

  static final class Builder {
    private final PolicyEntity policy;
    private final Set<MetadataObject> sources = new LinkedHashSet<>();
    private boolean directMatch;

    Builder(PolicyEntity policy) {
      this.policy = policy;
    }

    void record(boolean matches, boolean inherited, MetadataObject source) {
      if (!matches) {
        return;
      }
      sources.add(source);
      directMatch |= !inherited;
    }

    boolean matched() {
      return !sources.isEmpty();
    }

    MatchedPolicy build() {
      return new MatchedPolicy(policy.copyWithInherited(!directMatch), new ArrayList<>(sources));
    }
  }
}
