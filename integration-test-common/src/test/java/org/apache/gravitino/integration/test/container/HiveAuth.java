/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

/** How clients authenticate to the Hive a {@link HiveFixture} runs. */
public enum HiveAuth {
  /** No authentication. */
  NONE,
  /** Kerberos, from the Kerberos Hive image with its own KDC. */
  KERBEROS,
  /**
   * Ranger plugins for Hive and HDFS that report to the run's Ranger server, under the repositories
   * {@link RangerContainer#HIVE_REPOSITORY} and {@link RangerContainer#HDFS_REPOSITORY}. Hive3
   * only. Starts the Ranger server first, and starts lazily: a test asks for it after creating the
   * Ranger services.
   */
  RANGER
}
