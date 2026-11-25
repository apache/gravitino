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
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Names the local keytab files that Kerberos logins read, and keeps them in a directory of their
 * own.
 *
 * <p>The JDK keeps the keys of every keytab path it reads until the JVM exits, and returns them
 * while the file at the path keeps its modification time or is not a valid keytab. So a name is
 * reused only for the same keytab URI and content, and for a link to a local file the same
 * modification time, once its previous user deletes the file and releases the slot. The server
 * loads this class once for all catalog class loaders, so names are also reused after a catalog is
 * reloaded.
 *
 * <p>The first call to {@link #directory()} of each copy of this class locks its directory while
 * the copy is loaded. A JVM that exits without deleting its files leaves the lock free, and a later
 * JVM on a host with the same name and working directory deletes them. Files stay when the host
 * name changes or cannot be resolved, when the JVM exits before it locks its directory, when the
 * directory cannot be locked, and when {@code keytabs} is a link or others may write to it. Where
 * the file system has no POSIX permissions, as on Windows, the permissions are not checked.
 */
public final class KeytabNames {

  private static final Logger LOG = LoggerFactory.getLogger(KeytabNames.class);

  private static final String ROOT = "keytabs";
  private static final Pattern DIRECTORY = Pattern.compile("gravitino-([0-9a-f]{16})-[0-9a-f]{16}");
  private static final String LOCK_PREFIX = ".lock-";
  private static final long RETRY_NANOS = TimeUnit.MINUTES.toNanos(1);
  // Tells apart the JVMs that share a working directory. The copies of this class in one JVM share
  // it through a system property, so that none opens the lock file of another: closing a channel
  // on a file releases every lock the JVM holds on it.
  private static final String JVM = jvmId();
  // Tells apart the copies of this class in a JVM.
  private static final String TOKEN = randomId();
  // A failed login reports the keytab path, so a name holds a keyed hash of the keytab URI and
  // content, which tells nothing about the file, nor whether two URIs hold the same bytes.
  private static final HashFunction HASH =
      Hashing.hmacSha256(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
  // The used slots of each prefix, guarded by the map.
  private static final Map<String, BitSet> SLOTS = new HashMap<>();
  // The channels of this copy's lock file and of the lock files that other copies in this JVM
  // hold. They stay open: closing a channel releases the locks the JVM holds on its file. Guarded
  // by the list, as are the fields below.
  private static final List<FileChannel> CHANNELS = new ArrayList<>();
  private static boolean locked;
  private static boolean swept;
  private static boolean claiming;
  private static boolean warned;
  private static boolean tried;
  private static long lastClaimNanos;

  private KeytabNames() {}

  /**
   * Returns the directory of the keytab files of this copy of the class, which it locks and, once
   * locked, deletes the directories of the JVMs on this host that exited without deleting theirs.
   * That runs on a daemon thread, which the caller does not wait for. After a failure, it is tried
   * again by the first call a minute later.
   *
   * @return the directory, which may not exist yet
   */
  public static File directory() {
    File dir = new File(ROOT, "gravitino-" + JVM + "-" + TOKEN);
    synchronized (CHANNELS) {
      if ((locked && swept)
          || claiming
          || (tried && System.nanoTime() - lastClaimNanos < RETRY_NANOS)) {
        return dir;
      }
      claiming = true;
      tried = true;
      lastClaimNanos = System.nanoTime();
    }
    try {
      Thread thread = new Thread(() -> claim(dir), "gravitino-keytab-directory");
      thread.setDaemon(true);
      // A catalog class loader stops, when it is closed, the threads whose context class loader is
      // its own.
      thread.setContextClassLoader(KeytabNames.class.getClassLoader());
      thread.start();
    } catch (Throwable t) {
      synchronized (CHANNELS) {
        claiming = false;
      }
      // The keytab files are written there all the same.
      LOG.warn("Failed to start locking the keytab directory {}", dir, t);
    }
    return dir;
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

  /**
   * Locks the directory. Its lock file is locked under another name first, so that a lock file that
   * exists is always locked while the JVM that made it runs.
   *
   * @param dir the directory
   * @param host the host name, which the lock file is named after
   * @return the channel that holds the lock, which must stay open
   * @throws IOException if the directory cannot be locked
   */
  @VisibleForTesting
  static FileChannel lock(File dir, String host) throws IOException {
    Files.createDirectories(dir.toPath());
    Path temp = Files.createTempFile(dir.toPath(), ".lock", ".tmp");
    FileChannel channel = null;
    try {
      channel = FileChannel.open(temp, StandardOpenOption.READ, StandardOpenOption.WRITE);
      if (channel.tryLock() == null) {
        throw new IOException("Failed to lock " + temp);
      }
      Files.move(temp, dir.toPath().resolve(LOCK_PREFIX + host), StandardCopyOption.ATOMIC_MOVE);
      return channel;
    } catch (IOException | RuntimeException e) {
      if (channel != null) {
        closeQuietly(channel, e);
      }
      try {
        Files.deleteIfExists(temp);
      } catch (IOException | RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  /**
   * Deletes the keytab directories under the root that other JVMs on this host left behind: those
   * whose lock file for this host is not locked. Links in them are deleted, not their targets.
   * Nothing is deleted under a root that others than its owner and group may write to, where a
   * directory could be replaced by a link while it is deleted.
   *
   * @param root the directory that holds the keytab directories
   * @param jvm the id of this JVM, whose directories are never opened
   * @param host the host name of this JVM
   * @param held receives the channels on the lock files that this JVM holds, which must stay open
   * @throws IOException if the root cannot be listed
   */
  @VisibleForTesting
  static void sweep(File root, String jvm, String host, List<FileChannel> held) throws IOException {
    if (!Files.isDirectory(root.toPath(), LinkOption.NOFOLLOW_LINKS) || othersMayWrite(root)) {
      LOG.debug("Not deleting the keytab directories under {}, a link or writable by others", root);
      return;
    }
    try (DirectoryStream<Path> dirs = Files.newDirectoryStream(root.toPath())) {
      for (Path dir : dirs) {
        Matcher name = DIRECTORY.matcher(dir.getFileName().toString());
        Path lock = dir.resolve(LOCK_PREFIX + host);
        if (!name.matches()
            || name.group(1).equals(jvm)
            || !Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)
            || !Files.isRegularFile(lock, LinkOption.NOFOLLOW_LINKS)) {
          continue;
        }
        try {
          deleteIfUnlocked(dir, lock, held);
        } catch (IOException | RuntimeException e) {
          LOG.debug("Failed to delete the keytab directory {}", dir, e);
        }
      }
    }
  }

  private static void claim(File dir) {
    try {
      // Resolving the host name may take a while, so it runs here.
      String host = host();
      if (!isLocked()) {
        FileChannel channel = lock(dir, host);
        synchronized (CHANNELS) {
          CHANNELS.add(channel);
          locked = true;
        }
        // The lock file stays after the JVM exits, so that the next JVM on this host deletes a
        // keytab file that failed to be deleted at exit, and then the directory.
      }
      List<FileChannel> held = new ArrayList<>();
      try {
        sweep(dir.getAbsoluteFile().getParentFile(), JVM, host, held);
      } finally {
        synchronized (CHANNELS) {
          CHANNELS.addAll(held);
        }
      }
      synchronized (CHANNELS) {
        swept = true;
      }
    } catch (IOException | RuntimeException e) {
      boolean first;
      synchronized (CHANNELS) {
        first = !warned;
        warned = true;
      }
      if (first) {
        LOG.warn("Failed to lock the keytab directory {} or to clean up the others", dir, e);
      } else {
        LOG.debug("Failed to lock the keytab directory {} or to clean up the others", dir, e);
      }
    } finally {
      synchronized (CHANNELS) {
        claiming = false;
      }
    }
  }

  @VisibleForTesting
  static String host() throws IOException {
    return InetAddress.getLocalHost().getHostName().replaceAll("[^A-Za-z0-9._-]", "_");
  }

  private static boolean othersMayWrite(File root) throws IOException {
    try {
      return Files.getPosixFilePermissions(root.toPath(), LinkOption.NOFOLLOW_LINKS)
          .contains(PosixFilePermission.OTHERS_WRITE);
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }

  private static String jvmId() {
    String id = randomId();
    Object shared = System.getProperties().putIfAbsent(KeytabNames.class.getName() + ".jvm", id);
    // A value set by anyone else is not used, so that the directory keeps its name.
    return shared instanceof String && ((String) shared).matches("[0-9a-f]{16}")
        ? (String) shared
        : id;
  }

  private static String randomId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
  }

  private static void closeQuietly(FileChannel channel, Exception failure) {
    try {
      channel.close();
    } catch (IOException | RuntimeException suppressed) {
      failure.addSuppressed(suppressed);
    }
  }

  private static boolean isLocked() {
    synchronized (CHANNELS) {
      return locked;
    }
  }

  private static void deleteIfUnlocked(Path dir, Path lock, List<FileChannel> held)
      throws IOException {
    FileChannel channel =
        FileChannel.open(
            lock, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    FileLock acquired;
    try {
      acquired = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      // Another copy of this class in this JVM holds it: one that is deleting the same directory,
      // or one whose id the system property did not give.
      held.add(channel);
      return;
    } catch (IOException | RuntimeException e) {
      closeQuietly(channel, e);
      throw e;
    }
    try {
      if (acquired == null) {
        // Another process holds it.
        return;
      }
      try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
        for (Path file : files) {
          if (!file.equals(lock) && !Files.isDirectory(file, LinkOption.NOFOLLOW_LINKS)) {
            Files.deleteIfExists(file);
          }
        }
      }
      Files.deleteIfExists(lock);
    } finally {
      channel.close();
    }
    try {
      Files.deleteIfExists(dir);
    } catch (DirectoryNotEmptyException e) {
      LOG.debug("Kept the keytab directory {}, which holds directories", dir);
    }
  }
}
