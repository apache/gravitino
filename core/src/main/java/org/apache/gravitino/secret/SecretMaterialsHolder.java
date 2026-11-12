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
 * Mutable holder for secrets an alter wrote, pending versions to promote after commit, and replaced
 * write-through versions / URNs to delete after commit.
 *
 * <p>Returned by {@code SecretAlterChanges.prepare*Changes} and used by catalog, schema and fileset
 * alter paths.
 */
public final class SecretMaterialsHolder {
  private static final Logger LOG = LoggerFactory.getLogger(SecretMaterialsHolder.class);

  private List<SecretMaterial> materials = List.of();
  private List<SecretVersion> pendingVersions = List.of();
  private List<SecretVersion> replacedVersions = List.of();
  private List<SecretUrn> replacedUrns = List.of();

  /** Returns written secret materials (full URN writes rolled back with {@code deleteSecret}). */
  public List<SecretMaterial> get() {
    return materials;
  }

  /** Records written secret materials. */
  public void set(List<SecretMaterial> materials) {
    this.materials = materials;
  }

  /**
   * Returns pending versions written without changing the current readable value. Promoted after
   * commit; deleted on rollback.
   */
  public List<SecretVersion> getPendingVersions() {
    return pendingVersions;
  }

  /** Records pending secret versions. */
  public void setPendingVersions(List<SecretVersion> pendingVersions) {
    this.pendingVersions = pendingVersions;
  }

  /**
   * Returns versions replaced or removed while the URN may be rewritten in the same or a later
   * alter. Deleted with {@code deleteSecretVersion} after commit so a newer current version is
   * kept.
   */
  public List<SecretVersion> getReplacedVersions() {
    return replacedVersions;
  }

  /** Records replaced secret versions for post-commit deletion. */
  public void setReplacedVersions(List<SecretVersion> replacedVersions) {
    this.replacedVersions = replacedVersions;
  }

  /**
   * Returns write-through URNs replaced by a different URN (for example a cross-provider bind).
   * Deleted with {@code deleteSecret} after commit.
   */
  public List<SecretUrn> getReplacedUrns() {
    return replacedUrns;
  }

  /** Records replaced write-through URNs for post-commit deletion. */
  public void setReplacedUrns(List<SecretUrn> replacedUrns) {
    this.replacedUrns = replacedUrns;
  }

  /**
   * Copies bookkeeping from a prepare result into this holder (for use inside {@code store.update}
   * lambdas).
   *
   * @param prepared holder returned by prepare
   */
  public void copyFrom(SecretMaterialsHolder prepared) {
    this.materials = prepared.materials;
    this.pendingVersions = prepared.pendingVersions;
    this.replacedVersions = prepared.replacedVersions;
    this.replacedUrns = prepared.replacedUrns;
  }

  /**
   * After a successful alter commit: promotes pending versions to current, then deletes replaced
   * versions and URNs. Best effort; must not fail the already-committed alter.
   *
   * @param secretManager secret manager
   */
  public void commit(SecretManager secretManager) {
    secretManager.promoteSecretVersions(pendingVersions);
    secretManager.rollbackSecretVersions(replacedVersions);
    for (SecretUrn urn : replacedUrns) {
      try {
        secretManager.getRegistry().getProvider(urn.providerName()).deleteSecret(urn);
      } catch (Exception e) {
        LOG.warn("Failed to delete replaced secret {} after alter commit", urn, e);
      }
    }
  }

  /**
   * Rolls back materials written during a failed alter: pending versions and full URN writes.
   *
   * @param secretManager secret manager
   */
  public void rollback(SecretManager secretManager) {
    secretManager.rollbackSecretVersions(pendingVersions);
    secretManager.rollbackSecrets(materials);
  }
}
