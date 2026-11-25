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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestKeytabNames {

  private static final String JVM = "0123456789abcdef";
  private static final String OTHER_JVM = "fedcba9876543210";

  @Test
  public void testPrefixDependsOnIdentifierUriContentAndModificationTime() {
    byte[] content = "keys".getBytes(StandardCharsets.UTF_8);
    FileTime modified = FileTime.fromMillis(1_000);
    String prefix = KeytabNames.prefix("id", "file:///a.keytab", content, null);
    Assertions.assertEquals(
        prefix, KeytabNames.prefix("id", "file:///a.keytab", content.clone(), null));
    Assertions.assertTrue(prefix.startsWith("id-"), prefix);
    Assertions.assertNotEquals(
        prefix, KeytabNames.prefix("other", "file:///a.keytab", content, null));
    Assertions.assertNotEquals(prefix, KeytabNames.prefix("id", "file:///b.keytab", content, null));
    Assertions.assertNotEquals(
        prefix,
        KeytabNames.prefix(
            "id", "file:///a.keytab", "other".getBytes(StandardCharsets.UTF_8), null));
    // The URI length is hashed too, so moving bytes between the URI and the content changes it.
    Assertions.assertNotEquals(
        KeytabNames.prefix("id", "ab", "c".getBytes(StandardCharsets.UTF_8), null),
        KeytabNames.prefix("id", "a", "bc".getBytes(StandardCharsets.UTF_8), null));
    // A link is named after the modification time of its target too.
    String linked = KeytabNames.prefix("id", "file:///a.keytab", content, modified);
    Assertions.assertNotEquals(prefix, linked);
    Assertions.assertEquals(
        linked, KeytabNames.prefix("id", "file:///a.keytab", content, FileTime.fromMillis(1_000)));
    Assertions.assertNotEquals(
        linked, KeytabNames.prefix("id", "file:///a.keytab", content, FileTime.fromMillis(1_001)));
  }

  @Test
  public void testReservesTheLowestFreeSlot() {
    String prefix = KeytabNames.prefix("slots", "file:///slots.keytab", new byte[] {1}, null);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    Assertions.assertEquals(2, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 1);
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    KeytabNames.release(prefix, 1);
    KeytabNames.release(prefix, 2);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    // Releasing a slot of a prefix that holds none does nothing.
    KeytabNames.release(prefix, 5);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
  }

  // Holds the channels a sweep in this test JVM keeps open, which must not be closed.
  private static final List<FileChannel> HELD = new ArrayList<>();

  @Test
  public void testDirectoryIsLockedAndOthersLeftOnThisHostAreDeleted() throws Exception {
    // Only the first call in this test JVM locks the directory and deletes the others, unless the
    // keytabs directory of the working directory is writable by others.
    // Left in the working directory by another JVM on this host that exited.
    File exited = new File("keytabs", "gravitino-" + OTHER_JVM + "-" + id());
    Files.createDirectories(exited.toPath());
    Files.write(new File(exited, ".lock-" + KeytabNames.host()).toPath(), new byte[0]);
    Files.write(new File(exited, "gravitino-id-hash-0").toPath(), new byte[] {1});

    File dir = KeytabNames.directory();
    Assertions.assertEquals("keytabs", dir.getParent());
    Assertions.assertTrue(
        dir.getName().matches("gravitino-[0-9a-f]{16}-[0-9a-f]{16}"), dir.getName());
    Assertions.assertEquals(dir, KeytabNames.directory());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (exited.exists() || !new File(dir, ".lock-" + KeytabNames.host()).exists()) {
      Assertions.assertTrue(System.nanoTime() < deadline, "the directory was not claimed");
      Thread.sleep(10);
    }
    // The lock stays held: as seen by another JVM id, the directory is locked by this JVM.
    List<FileChannel> held = new ArrayList<>();
    try {
      KeytabNames.sweep(new File("keytabs"), OTHER_JVM, KeytabNames.host(), held);
      Assertions.assertTrue(dir.exists());
      Assertions.assertFalse(held.isEmpty());
    } finally {
      HELD.addAll(held);
    }
  }

  @Test
  public void testSweepDeletesTheDirectoriesOfExitedJvmsOnThisHost(
      @TempDir File root, @TempDir File outside) throws Exception {
    File target = new File(outside, "source.keytab");
    Files.write(target.toPath(), new byte[] {1});
    // Left by a JVM that exited: its lock file is not locked.
    File exited = directory(root, OTHER_JVM);
    Files.write(new File(exited, ".lock-host").toPath(), new byte[0]);
    Files.write(new File(exited, "gravitino-id-hash-0").toPath(), new byte[] {2});
    Files.createSymbolicLink(new File(exited, "gravitino-id-hash-1").toPath(), target.toPath());

    List<FileChannel> held = new ArrayList<>();
    KeytabNames.sweep(root, JVM, "host", held);

    Assertions.assertFalse(exited.exists());
    // A link is deleted, not the file it links to.
    Assertions.assertTrue(target.exists());
    Assertions.assertTrue(held.isEmpty());
  }

  @Test
  public void testSweepKeepsWhatIsNotADirectoryOfAnExitedJvmOnThisHost(
      @TempDir File root, @TempDir File outside) throws Exception {
    // A directory of this JVM, which is never opened.
    File own = directory(root, JVM);
    Files.write(new File(own, ".lock-host").toPath(), new byte[0]);
    // A JVM on another host, whose locks this host may not see.
    File otherHost = directory(root, OTHER_JVM);
    Files.write(new File(otherHost, ".lock-other").toPath(), new byte[0]);
    // A JVM that has not renamed its lock file yet.
    File locking = directory(root, OTHER_JVM);
    Files.write(new File(locking, ".lock123.tmp").toPath(), new byte[0]);
    // A directory with another name.
    File otherName = new File(root, "gravitino-" + OTHER_JVM + "-" + id() + "-x");
    Files.createDirectory(otherName.toPath());
    Files.write(new File(otherName, ".lock-host").toPath(), new byte[0]);
    // A link to a directory of an exited JVM elsewhere.
    File linkedTarget = directory(outside, OTHER_JVM);
    Files.write(new File(linkedTarget, ".lock-host").toPath(), new byte[0]);
    File link = new File(root, "gravitino-" + OTHER_JVM + "-" + id());
    Files.createSymbolicLink(link.toPath(), linkedTarget.toPath());
    // The keytab files of other catalogs and of earlier versions.
    File hive = new File(root, "gravitino-" + id() + "-keytab");
    File earlier = new File(root, "gravitino-gravitino.v1.uid1");
    File plain = new File(root, "gravitino-" + OTHER_JVM + "-" + id());
    for (File file : new File[] {hive, earlier, plain}) {
      Files.write(file.toPath(), new byte[] {1});
    }

    List<FileChannel> held = new ArrayList<>();
    KeytabNames.sweep(root, JVM, "host", held);

    for (File file :
        new File[] {own, otherHost, locking, otherName, link, linkedTarget, hive, earlier, plain}) {
      Assertions.assertTrue(file.exists(), file.getName());
    }
    Assertions.assertTrue(new File(own, ".lock-host").exists());
    Assertions.assertTrue(new File(otherName, ".lock-host").exists());
    Assertions.assertTrue(new File(linkedTarget, ".lock-host").exists());
    Assertions.assertTrue(held.isEmpty());
  }

  @Test
  public void testSweepKeepsTheDirectoryThatAnotherProcessLocked(@TempDir File root)
      throws Exception {
    File other = directory(root, OTHER_JVM);
    File keytab = new File(other, "gravitino-id-hash-0");
    Files.write(keytab.toPath(), new byte[] {1});
    File locked = new File(root, "locked");
    File log = new File(root, "lock-holder.log");
    Process process =
        new ProcessBuilder(
                new File(System.getProperty("java.home"), "bin/java").getPath(),
                "-cp",
                System.getProperty("java.class.path"),
                LockHolder.class.getName(),
                other.getPath(),
                locked.getPath())
            .redirectErrorStream(true)
            .redirectOutput(log)
            .start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!locked.exists()) {
        if (!process.isAlive()) {
          Assertions.fail(
              "the lock holder exited: "
                  + new String(Files.readAllBytes(log.toPath()), StandardCharsets.UTF_8));
        }
        Assertions.assertTrue(System.nanoTime() < deadline, "the lock holder did not lock");
        Thread.sleep(10);
      }
      List<FileChannel> held = new ArrayList<>();
      KeytabNames.sweep(root, JVM, "host", held);
      Assertions.assertTrue(keytab.exists());
      Assertions.assertTrue(held.isEmpty());
    } finally {
      process.destroyForcibly();
    }
    Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the lock holder did not exit");
    // Killing the process frees its lock, and the next sweep deletes what it left.
    KeytabNames.sweep(root, JVM, "host", new ArrayList<>());
    Assertions.assertFalse(other.exists());
  }

  @Test
  public void testSweepKeepsTheDirectoryThatAnotherCopyInThisJvmLocked(@TempDir File root)
      throws Exception {
    // As if a copy of this class did not see the id of this JVM.
    File other = directory(root, OTHER_JVM);
    FileChannel lock = KeytabNames.lock(other, "host");
    try {
      File keytab = new File(other, "gravitino-id-hash-0");
      Files.write(keytab.toPath(), new byte[] {1});

      List<FileChannel> held = new ArrayList<>();
      KeytabNames.sweep(root, JVM, "host", held);

      Assertions.assertTrue(keytab.exists());
      // Closing the channel would release the lock of the other copy.
      Assertions.assertEquals(1, held.size());
      Assertions.assertTrue(held.get(0).isOpen());
      for (FileChannel channel : held) {
        channel.close();
      }
    } finally {
      lock.close();
    }
  }

  @Test
  public void testSweepDeletesNothingUnderARootOthersMayWrite(@TempDir File root) throws Exception {
    Assumptions.assumeTrue(
        FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "POSIX only");
    File exited = directory(root, OTHER_JVM);
    Files.write(new File(exited, ".lock-host").toPath(), new byte[0]);
    Files.setPosixFilePermissions(root.toPath(), PosixFilePermissions.fromString("rwxrwxrwx"));

    KeytabNames.sweep(root, JVM, "host", new ArrayList<>());

    Assertions.assertTrue(exited.exists());
  }

  @Test
  public void testLockFileIsNamedOnceLocked(@TempDir File root) throws Exception {
    File dir = directory(root, OTHER_JVM);
    try (FileChannel lock = KeytabNames.lock(dir, "host")) {
      Assertions.assertArrayEquals(new String[] {".lock-host"}, dir.list());
    }
  }

  private static File directory(File root, String jvm) throws IOException {
    Path dir = root.toPath().resolve("gravitino-" + jvm + "-" + id());
    return Files.createDirectory(dir).toFile();
  }

  private static String id() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
  }

  /**
   * Locks a directory with {@link KeytabNames#lock(File, String)} until it is killed or the process
   * that started it exits, which closes its input.
   */
  public static final class LockHolder {
    public static void main(String[] args) throws Exception {
      try (FileChannel channel = KeytabNames.lock(new File(args[0]), "host")) {
        Files.createFile(new File(args[1]).toPath());
        while (System.in.read() != -1) {
          // Waits for the end of the input.
        }
      }
    }
  }
}
