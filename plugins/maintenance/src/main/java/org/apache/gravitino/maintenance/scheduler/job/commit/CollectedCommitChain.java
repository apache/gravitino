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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.maintenance.scheduler.ResolvedTableRef;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Result of {@link CommitChainCollector#collect(long)}: onCommit policies in chain order plus the
 * resolved Spark table ref used to build the enqueue-time spec (single table-metadata lookup).
 */
public final class CollectedCommitChain {

  private final List<PolicyEntity> policies;
  private final ResolvedTableRef tableRef;

  /**
   * @param policies onCommit policies in chain order (may be empty)
   * @param tableRef resolved Spark table ref for the table id
   */
  public CollectedCommitChain(List<PolicyEntity> policies, ResolvedTableRef tableRef) {
    this.policies = List.copyOf(Preconditions.checkNotNull(policies, "policies"));
    this.tableRef = Preconditions.checkNotNull(tableRef, "tableRef");
  }

  /**
   * @return onCommit policies in chain order
   */
  public List<PolicyEntity> policies() {
    return policies;
  }

  /**
   * @return resolved Spark table ref
   */
  public ResolvedTableRef tableRef() {
    return tableRef;
  }
}
