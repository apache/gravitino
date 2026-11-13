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
import javax.annotation.Nullable;

/**
 * Write-through secret versions that a prepared entity alter wrote or stopped referencing.
 *
 * <p>Created by the {@code SecretAlterChanges.prepare*Changes} methods. After the entity alter
 * commits, {@link SecretManager#commitSecrets} applies them; if the alter fails, {@link
 * SecretManager#rollbackSecrets(SecretVersionChanges)} deletes the written versions.
 */
public final class SecretVersionChanges {

  /** Secret version changes with nothing written or released. */
  public static final SecretVersionChanges EMPTY = new SecretVersionChanges(List.of(), List.of());

  private final List<WrittenVersion> written;
  private final List<Version> released;

  SecretVersionChanges(List<WrittenVersion> written, List<Version> released) {
    this.written = List.copyOf(written);
    this.released = List.copyOf(released);
  }

  /**
   * Returns a copy without the released versions, so committing it keeps them.
   *
   * <p>For an alter whose entity update may not have been stored, where the entity can still
   * reference the released URNs.
   *
   * @return changes with the same written versions and nothing released
   */
  public SecretVersionChanges withoutReleased() {
    return new SecretVersionChanges(written, List.of());
  }

  /** Versions written by the alter, to make current on commit and delete on rollback. */
  List<WrittenVersion> written() {
    return written;
  }

  /** Current versions of URNs the altered entity no longer references, to delete on commit. */
  List<Version> released() {
    return released;
  }

  /** One version of a write-through secret. */
  static final class Version {
    private final SecretUrn urn;
    private final String id;

    Version(SecretUrn urn, String id) {
      this.urn = urn;
      this.id = id;
    }

    SecretUrn urn() {
      return urn;
    }

    String id() {
      return id;
    }
  }

  /** A written version and the version of the same URN that was current before the write. */
  static final class WrittenVersion {
    private final Version version;
    @Nullable private final String previousId;

    WrittenVersion(Version version, @Nullable String previousId) {
      this.version = version;
      this.previousId = previousId;
    }

    Version version() {
      return version;
    }

    @Nullable
    String previousId() {
      return previousId;
    }
  }
}
