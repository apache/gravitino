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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutable holder so {@code store.update} lambdas can record written secrets for rollback and
 * replaced write-through URNs for post-commit deletion.
 *
 * <p>Used by catalog, schema, and fileset alter paths that prepare secrets inside {@code
 * store.update} and roll back on failure.
 */
public final class SecretMaterialsHolder {
  private static final Logger LOG = LoggerFactory.getLogger(SecretMaterialsHolder.class);

  private List<SecretMaterial> materials = List.of();
  private List<SecretUrn> replacedUrns = List.of();

  /** Returns written secret materials. */
  public List<SecretMaterial> get() {
    return materials;
  }

  /** Records written secret materials. */
  public void set(List<SecretMaterial> materials) {
    this.materials = materials;
  }

  /**
   * Returns the URNs of write-through secrets replaced or removed by the prepared changes. They are
   * deleted only after the alter commits; on failure they must stay resolvable because the
   * persisted entity still references them.
   */
  public List<SecretUrn> getReplacedUrns() {
    return replacedUrns;
  }

  /** Records the URNs of replaced write-through secrets for post-commit deletion. */
  public void setReplacedUrns(List<SecretUrn> replacedUrns) {
    this.replacedUrns = replacedUrns;
  }

  /**
   * Deletes the replaced write-through secrets after the alter commits. Best effort: the committed
   * entity no longer references them, so a failure only leaks material and must not fail the
   * committed alter.
   *
   * @param secretManager secret manager used for deletion
   */
  public void deleteReplaced(SecretManager secretManager) {
    try {
      secretManager.deleteSecrets(replacedUrns);
    } catch (RuntimeException e) {
      LOG.warn("Failed to delete replaced secrets after a committed alter", e);
    }
  }
}
