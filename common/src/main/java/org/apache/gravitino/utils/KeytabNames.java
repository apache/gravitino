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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Names the local keytab files that Kerberos logins read.
 *
 * <p>The JDK keeps the keys of every keytab path it reads until the JVM exits, and returns them
 * while the file at the path keeps its modification time or is not a valid keytab. So a name is
 * reused only for the same keytab URI and content, once the file of its previous user is deleted.
 * The server loads this class once for all catalog class loaders, so names are also reused after a
 * catalog is reloaded.
 */
public final class KeytabNames {

  // Tells apart the keytab files of JVMs that share a working directory.
  private static final String TOKEN = UUID.randomUUID().toString().substring(0, 8);
  // A failed login reports the keytab path, so a name holds a keyed hash of the keytab URI and
  // content, which tells nothing about the file, nor whether two URIs hold the same bytes.
  private static final HashFunction HASH =
      Hashing.hmacSha256(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
  // The used slots of each prefix, guarded by the map.
  private static final Map<String, BitSet> SLOTS = new HashMap<>();

  private KeytabNames() {}

  /**
   * Returns the token of this JVM, which every keytab file name of the JVM holds.
   *
   * @return the token
   */
  public static String token() {
    return TOKEN;
  }

  /**
   * Returns the prefix of the names of a keytab file with the given content.
   *
   * @param identifier the identifier of the entity that the keytab file is for
   * @param keytabUri the URI the keytab is fetched from
   * @param content the content of the fetched keytab
   * @return the prefix, which holds the identifier, the token and a keyed hash of the URI and
   *     content
   */
  public static String prefix(String identifier, String keytabUri, byte[] content) {
    byte[] uri = keytabUri.getBytes(StandardCharsets.UTF_8);
    return identifier
        + "-"
        + TOKEN
        + "-"
        + HASH.newHasher().putInt(uri.length).putBytes(uri).putBytes(content).hash();
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
}
