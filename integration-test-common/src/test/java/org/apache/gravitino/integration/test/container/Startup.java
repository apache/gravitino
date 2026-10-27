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

/**
 * When a fixture's container starts.
 *
 * <p>A field marked {@link FixtureContainer} is filled before {@code @BeforeAll}, so it starts its
 * container then either way; {@link #LAZY} only defers containers that tests receive as parameters.
 */
public enum Startup {
  /** Before the class's {@code @BeforeAll} methods, so start failures show up in class setup. */
  EAGER,
  /**
   * When a test or lifecycle method first asks for it, for containers that need something a test
   * sets up first, such as Trino needing the Gravitino server's port.
   */
  LAZY
}
