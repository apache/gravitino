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

import java.util.List;

/**
 * The nodes of a ClickHouse cluster, injected for {@code @ClickHouseFixture(nodes = 3)}. A test
 * that asks for a {@link ClickHouseContainer} instead receives the first node. {@code nodes} holds
 * every node, first node first.
 */
public record ClickHouseCluster(List<ClickHouseContainer> nodes) {

  /** Copies the node list. */
  public ClickHouseCluster {
    nodes = List.copyOf(nodes);
  }

  /**
   * Returns the first node, which tests use to run cluster-wide statements.
   *
   * @return the first node
   */
  public ClickHouseContainer first() {
    return nodes.get(0);
  }
}
