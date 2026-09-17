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

import java.util.List;

/**
 * Mutable holder so {@code store.update} lambdas can record written secrets for rollback.
 *
 * <p>Used by catalog, schema, and fileset alter paths that prepare secrets inside {@code
 * store.update} and roll back on failure.
 */
public final class SecretMaterialsHolder {

  private List<SecretMaterial> materials = List.of();

  /** Returns written secret materials. */
  public List<SecretMaterial> get() {
    return materials;
  }

  /** Records written secret materials. */
  public void set(List<SecretMaterial> materials) {
    this.materials = materials;
  }
}
