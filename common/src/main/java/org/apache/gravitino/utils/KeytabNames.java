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
package org.apache.gravitino.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Names the local keytab files that Kerberos logins read.
 *
 * <p>The JDK keeps the keys of every keytab path it reads until the JVM exits, and returns them
 * while the file at the path keeps its modification time or is not a valid keytab. So a name is
 * reused only for the same keytab URI and content, and for a link to a local file the same
 * modification time, once its previous user deletes the file and releases the slot. The server
 * loads this class once for all catalog class loaders, so names are also reused after a catalog is
 * reloaded.
 *
 * <p>Files that a JVM leaves when it exits without running its shutdown hooks are not deleted.
 */
public final class KeytabNames {

  private static final String ROOT = "keytabs";
  // A failed login reports the keytab path, so a name holds a keyed hash of the keytab URI and
  // content, which tells nothing about the file, nor whether two URIs hold the same bytes. The key
  // is random, so JVMs that share a working directory do not share names.
  private static final HashFunction HASH =
      Hashing.hmacSha256(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
  // The used slots of each prefix, guarded by the map.
  private static final Map<String, BitSet> SLOTS = new HashMap<>();

  private KeytabNames() {}

  /**
   * Returns the directory of the keytab files, {@code keytabs} under the working directory.
   *
   * @return the directory, which may not exist yet
   */
  public static File directory() {
    return new File(ROOT);
  }

  /**
   * Returns the prefix of the names of a keytab file with the given content.
   *
   * @param identifier the identifier of the entity that the keytab file is for
   * @param keytabUri the URI the keytab is fetched from
   * @param content the content of the fetched keytab
   * @param modified the modification time of the local file that the keytab file links to, read
   *     before the content, or null for a copy
   * @return the prefix, which holds the identifier and a keyed hash of the URI, the content and the
   *     modification time
   */
  public static String prefix(
      String identifier, String keytabUri, byte[] content, @Nullable FileTime modified) {
    byte[] uri = keytabUri.getBytes(StandardCharsets.UTF_8);
    Hasher hasher = HASH.newHasher().putInt(uri.length).putBytes(uri);
    if (modified == null) {
      hasher.putBoolean(false);
    } else {
      Instant time = modified.toInstant();
      hasher.putBoolean(true).putLong(time.getEpochSecond()).putInt(time.getNano());
    }
    return identifier + "-" + hasher.putBytes(content).hash();
  }

  /**
   * Reserves the lowest slot of the prefix that no keytab file holds.
   *
   * @param prefix the prefix of the names
   * @return the slot, which the name ends with
   */
  public static int reserve(String prefix) {
    synchronized (SLOTS) {
      BitSet used = SLOTS.computeIfAbsent(prefix, p -> new BitSet());
      int slot = used.nextClearBit(0);
      used.set(slot);
      return slot;
    }
  }

  /**
   * Frees a slot once the keytab file that held it is deleted.
   *
   * @param prefix the prefix of the names
   * @param slot the slot that {@link #reserve(String)} returned
   */
  public static void release(String prefix, int slot) {
    synchronized (SLOTS) {
      BitSet used = SLOTS.get(prefix);
      if (used == null) {
        return;
      }
      used.clear(slot);
      if (used.isEmpty()) {
        SLOTS.remove(prefix);
      }
    }
  }

  @VisibleForTesting
  static boolean hasReservedSlots(String prefix) {
    synchronized (SLOTS) {
      return SLOTS.containsKey(prefix);
    }
  }
}
