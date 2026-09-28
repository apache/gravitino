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
package org.apache.gravitino.maintenance.optimizer.recommender.handler;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;

/** Immutable rewrite request retaining the spec whose statistics were evaluated. */
public final class ManifestRewriteJobContext implements JobExecutionContext {
  private final NameIdentifier name;
  private final int specId;
  @Nullable private final Boolean useCaching;

  /**
   * Creates a request for one resolved partition spec.
   *
   * @param name normalized catalog/schema/table identifier
   * @param specId resolved existing partition spec
   * @param useCaching optional Iceberg caching setting
   */
  public ManifestRewriteJobContext(NameIdentifier name, int specId, @Nullable Boolean useCaching) {
    Preconditions.checkArgument(specId >= 0, "spec_id must be >= 0");
    this.name = Objects.requireNonNull(name, "name");
    this.specId = specId;
    this.useCaching = useCaching;
  }

  /**
   * Returns the resolved spec, never the table's subsequently changed default.
   *
   * @return resolved partition spec ID
   */
  public int specId() {
    return specId;
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return name;
  }

  @Override
  public String jobTemplateName() {
    return IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE;
  }

  @Override
  public Map<String, String> jobOptions() {
    return useCaching == null
        ? Map.of("spec_id", Integer.toString(specId))
        : Map.of("spec_id", Integer.toString(specId), "use_caching", useCaching.toString());
  }
}
