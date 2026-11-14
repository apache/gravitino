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
package org.apache.gravitino.secret;

import com.google.common.base.Preconditions;

/**
 * Mutable holder so {@code store.update} lambdas can record secret version changes.
 *
 * <p>Used by catalog, schema, and fileset alter paths that stage secret versions inside {@code
 * store.update}, commit them after the update and roll them back on failure.
 */
public final class SecretMaterialsHolder {

  private SecretVersionChanges changes = SecretVersionChanges.EMPTY;

  /**
   * Returns the recorded secret version changes.
   *
   * @return the recorded changes, {@link SecretVersionChanges#EMPTY} until {@link #set} is called
   */
  public SecretVersionChanges get() {
    return changes;
  }

  /**
   * Records the secret version changes of the alter.
   *
   * @param changes the changes returned by a {@code SecretAlterChanges.prepare*Changes} method
   */
  public void set(SecretVersionChanges changes) {
    this.changes = Preconditions.checkNotNull(changes, "changes must not be null");
  }
}
