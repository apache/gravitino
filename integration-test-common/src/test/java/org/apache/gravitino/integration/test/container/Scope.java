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
 * How long a fixture's container lives, and who shares it.
 *
 * <p>Use a narrower scope only for tests that change or break their container, such as tests that
 * add Kerberos principals: a private container costs a full start.
 */
public enum Scope {
  /** One container for the whole run, shared by every class that asks for the same spec. */
  RUN,
  /**
   * A private container for one test class, closed after the class. It never replaces the shared
   * container.
   */
  CLASS,
  /**
   * A private container for each test method, closed after the test. It never replaces the shared
   * container.
   */
  METHOD
}
