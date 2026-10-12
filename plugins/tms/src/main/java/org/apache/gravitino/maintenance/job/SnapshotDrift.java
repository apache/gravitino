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
package org.apache.gravitino.maintenance.job;

/** Snapshot drift checks after an in-job sample at {@code S0} (§4.3). */
public final class SnapshotDrift {

  private SnapshotDrift() {}

  /**
   * @param sampledSnapshotId snapshot id captured when metrics were sampled ({@code S0})
   * @param headSnapshotId current table HEAD snapshot id
   * @return whether HEAD has moved since the sample
   */
  public static boolean hasDrifted(long sampledSnapshotId, long headSnapshotId) {
    return sampledSnapshotId != headSnapshotId;
  }

  /**
   * @param sampledSnapshotId snapshot id captured when metrics were sampled ({@code S0})
   * @param head supplies current HEAD
   * @return whether HEAD has moved since the sample
   */
  public static boolean hasDrifted(long sampledSnapshotId, SnapshotHead head) {
    return hasDrifted(sampledSnapshotId, head.currentSnapshotId());
  }
}
