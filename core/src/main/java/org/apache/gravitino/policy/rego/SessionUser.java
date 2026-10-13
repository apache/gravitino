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
package org.apache.gravitino.policy.rego;

/** A symbolic reference to the trusted effective session user. */
final class SessionUser implements CanonicalExpression {
  SessionUser() {}

  @Override
  public void validate() throws IllegalArgumentException {}

  @Override
  public int depth() {
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SessionUser;
  }

  @Override
  public int hashCode() {
    return SessionUser.class.hashCode();
  }

  @Override
  public String toString() {
    return "SessionUser{}";
  }
}
