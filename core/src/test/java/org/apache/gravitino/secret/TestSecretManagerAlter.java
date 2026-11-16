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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.TestCatalogManager;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TestSecretManagerAlter {

  private static final long ID = 7L;
  private static final String KEY = "jdbc-password";

  /**
   * Records the versions it writes and deletes and counts version switches, fails every switch, and
   * fails version writes of the property key {@code broken}. For the key {@code applied} the switch
   * takes effect before failing; for {@code raced} another version is made current before it fails;
   * for {@code unknown} every lookup of the current version fails from the first switch until
   * {@code lookupFails} is cleared.
   */
  public static class FaultySecretsProvider extends InMemorySecretsProvider {
    private final List<String> writtenVersions = new ArrayList<>();
    private final List<String> deletedVersions = new ArrayList<>();
    private int switches;
    private boolean lookupFails;

    @Override
    public String writeSecretVersion(SecretUrn urn, String plaintext) {
      if ("broken".equals(urn.propertyKey())) {
        throw new IllegalStateException("injected write failure");
      }
      String version = super.writeSecretVersion(urn, plaintext);
      writtenVersions.add(urn + "@" + version);
      return version;
    }

    @Override
    public void setCurrentSecretVersion(SecretUrn urn, String version) {
      switches++;
      // For the key "applied" the switch takes effect before it fails, like a timed-out call.
      if ("applied".equals(urn.propertyKey())) {
        super.setCurrentSecretVersion(urn, version);
      }
      // For the key "raced" another writer makes its own version current first.
      if ("raced".equals(urn.propertyKey())) {
        super.setCurrentSecretVersion(urn, super.writeSecretVersion(urn, "other"));
      }
      // For the key "unknown" the current version cannot be looked up after the failed switch.
      if ("unknown".equals(urn.propertyKey())) {
        lookupFails = true;
      }
      throw new IllegalStateException("injected switch failure");
    }

    @Override
    @Nullable
    public String currentSecretVersion(SecretUrn urn) {
      if (lookupFails && "unknown".equals(urn.propertyKey())) {
        throw new IllegalStateException("injected lookup failure");
      }
      return super.currentSecretVersion(urn);
    }

    @Override
    public void deleteSecretVersion(SecretUrn urn, String version) {
      deletedVersions.add(urn + "@" + version);
      super.deleteSecretVersion(urn, version);
    }
  }

  /** A write-through provider that implements only the methods that existed before versions. */
  public static class UnversionedSecretsProvider extends InMemorySecretsProvider {
    @Override
    public String writeSecretVersion(SecretUrn urn, String plaintext) {
      throw new UnsupportedOperationException("unversioned");
    }

    @Override
    public void setCurrentSecretVersion(SecretUrn urn, String version) {
      throw new UnsupportedOperationException("unversioned");
    }

    @Override
    public void deleteSecretVersion(SecretUrn urn, String version) {
      throw new UnsupportedOperationException("unversioned");
    }

    @Override
    public String currentSecretVersion(SecretUrn urn) {
      throw new UnsupportedOperationException("unversioned");
    }
  }

  @Test
  void testFirstBindingIsReadableAfterPrepare() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> start = Map.of("jdbc-user", "root");
      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, start, bind(KEY, "memory", "s3cr3t"));
      Map<String, String> props = applied(start, result.getLeft());
      Assertions.assertTrue(
          SecretPropertyUtils.isWriteThroughForEntity(KEY, props.get(KEY), "catalog", ID));
      Assertions.assertEquals("s3cr3t", read(secretManager, props, KEY));

      secretManager.rollbackSecrets(result.getRight());
      Assertions.assertNull(currentVersion(secretManager, props.get(KEY)));
    }
  }

  @Test
  void testRotationSwitchesValueOnCommit() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      String oldVersion = currentVersion(secretManager, props.get(KEY));

      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, props, bind(KEY, "memory", "new"));
      Assertions.assertEquals(props, applied(props, result.getLeft()));
      Assertions.assertEquals("old", read(secretManager, props, KEY));

      secretManager.commitSecrets(result.getRight());
      Assertions.assertEquals("new", read(secretManager, props, KEY));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> provider(secretManager).setCurrentSecretVersion(urn(props, KEY), oldVersion));
    }
  }

  @Test
  void testRolledBackRotationKeepsOldValue() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      SecretVersionChanges prepared =
          prepare(secretManager, props, bind(KEY, "memory", "new")).getRight();

      secretManager.rollbackSecrets(prepared);
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      String written = prepared.written().get(0).version().id();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> provider(secretManager).setCurrentSecretVersion(urn(props, KEY), written));
    }
  }

  @Test
  void testSetPropertyRotatesWriteThroughSecret() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));

      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, props, CatalogChange.setProperty(KEY, "new"));
      Assertions.assertEquals(props, applied(props, result.getLeft()));
      Assertions.assertEquals("old", read(secretManager, props, KEY));

      secretManager.commitSecrets(result.getRight());
      Assertions.assertEquals("new", read(secretManager, props, KEY));
    }
  }

  @Test
  void testRemoveDeletesSecretOnlyOnCommit() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));

      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, props, CatalogChange.removeProperty(KEY));
      Assertions.assertFalse(applied(props, result.getLeft()).containsKey(KEY));
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      secretManager.rollbackSecrets(result.getRight());
      Assertions.assertEquals("old", read(secretManager, props, KEY));

      alter(secretManager, props, CatalogChange.removeProperty(KEY));
      Assertions.assertNull(currentVersion(secretManager, props.get(KEY)));
    }
  }

  @Test
  void testFailedRemoveAndRebindKeepsOldValue() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      CatalogChange[] changes = {CatalogChange.removeProperty(KEY), bind(KEY, "memory", "new")};

      SecretVersionChanges prepared = prepare(secretManager, props, changes).getRight();
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      secretManager.rollbackSecrets(prepared);
      Assertions.assertEquals("old", read(secretManager, props, KEY));

      Assertions.assertEquals(props, alter(secretManager, props, changes));
      Assertions.assertEquals("new", read(secretManager, props, KEY));
    }
  }

  @Test
  void testRebindAfterReferenceOrOtherProviderKeepsFinalSecret() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));

      Map<String, String> viaReference =
          alter(
              secretManager,
              props,
              CatalogChange.setSecretReference(
                  KEY, new SecretReference("reference", Map.of("path", "kv"))),
              bind(KEY, "memory", "x"));
      Assertions.assertEquals(props, viaReference);
      Assertions.assertEquals("x", read(secretManager, props, KEY));

      Map<String, String> viaOtherProvider =
          alter(secretManager, props, bind(KEY, "memory2", "y"), bind(KEY, "memory", "z"));
      Assertions.assertEquals(props, viaOtherProvider);
      Assertions.assertEquals("z", read(secretManager, props, KEY));
      Assertions.assertNull(currentVersion(secretManager, writeThroughUrn("memory2", KEY)));
    }
  }

  @Test
  void testBindThenRemoveDeletesSecret() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));

      Map<String, String> committed =
          alter(
              secretManager, props, bind(KEY, "memory", "new"), CatalogChange.removeProperty(KEY));
      Assertions.assertFalse(committed.containsKey(KEY));
      Assertions.assertNull(currentVersion(secretManager, props.get(KEY)));
    }
  }

  @Test
  void testSwitchToReferenceReleasesOldVersionOnCommit() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      CatalogChange toReference =
          CatalogChange.setSecretReference(
              KEY, new SecretReference("reference", Map.of("path", "kv")));

      Pair<CatalogChange[], SecretVersionChanges> first =
          prepare(secretManager, props, toReference);
      secretManager.rollbackSecrets(first.getRight());
      Assertions.assertEquals("old", read(secretManager, props, KEY));

      // The reference is committed, then another alter binds the key again before the first alter
      // deletes the released version.
      Pair<CatalogChange[], SecretVersionChanges> second =
          prepare(secretManager, props, toReference);
      Map<String, String> referenced = applied(props, second.getLeft());
      Map<String, String> rebound = alter(secretManager, referenced, bind(KEY, "memory", "new"));
      secretManager.commitSecrets(second.getRight());

      Assertions.assertEquals(props, rebound);
      Assertions.assertEquals("new", read(secretManager, rebound, KEY));

      Assertions.assertEquals(referenced, alter(secretManager, rebound, toReference));
      Assertions.assertNull(currentVersion(secretManager, rebound.get(KEY)));
    }
  }

  @Test
  void testBindThenReferenceWritesNothing() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      CatalogChange toReference =
          CatalogChange.setSecretReference(
              KEY, new SecretReference("reference", Map.of("path", "kv")));

      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, props, bind(KEY, "memory", "new"), toReference);
      Assertions.assertTrue(result.getRight().written().isEmpty());
      Map<String, String> referenced = applied(props, result.getLeft());
      Assertions.assertEquals("urn:gravitino-secret:reference:kv:" + KEY, referenced.get(KEY));

      secretManager.commitSecrets(result.getRight());
      Assertions.assertNull(currentVersion(secretManager, props.get(KEY)));
    }
  }

  @Test
  void testSwitchToOtherProviderReleasesOldUrnOnCommit() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));

      Pair<CatalogChange[], SecretVersionChanges> result =
          prepare(secretManager, props, bind(KEY, "memory2", "new"));
      Map<String, String> switched = applied(props, result.getLeft());
      secretManager.rollbackSecrets(result.getRight());
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      Assertions.assertNull(currentVersion(secretManager, switched.get(KEY)));

      Assertions.assertEquals(switched, alter(secretManager, props, bind(KEY, "memory2", "new")));
      Assertions.assertEquals("new", read(secretManager, switched, KEY));
      Assertions.assertNull(currentVersion(secretManager, props.get(KEY)));
    }
  }

  @Test
  void testAlterRepairsSecretWithMissingMaterial() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      provider(secretManager).deleteSecret(urn(props, KEY));

      alter(secretManager, props, bind(KEY, "memory", "rebound"));
      Assertions.assertEquals("rebound", read(secretManager, props, KEY));

      provider(secretManager).deleteSecret(urn(props, KEY));
      alter(secretManager, props, CatalogChange.setProperty(KEY, "set"));
      Assertions.assertEquals("set", read(secretManager, props, KEY));

      provider(secretManager).deleteSecret(urn(props, KEY));
      Assertions.assertFalse(
          alter(secretManager, props, CatalogChange.removeProperty(KEY)).containsKey(KEY));
    }
  }

  @Test
  void testFailedSwitchKeepsPreviousVersionAndCommitsTheRest() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props =
          alter(
              secretManager,
              Map.of(),
              bind(KEY, "faulty", "old"),
              bind("k2", "memory", "old2"),
              bind("other", "memory", "x"));

      Map<String, String> committed =
          alter(
              secretManager,
              props,
              bind(KEY, "faulty", "new"),
              bind("k2", "memory", "new2"),
              CatalogChange.removeProperty("other"));
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      Assertions.assertEquals("new2", read(secretManager, committed, "k2"));
      Assertions.assertNull(currentVersion(secretManager, props.get("other")));
    }
  }

  @Test
  void testFailedSwitchDeletesTheUnusedVersion() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "faulty", "old"));
      FaultySecretsProvider faulty = faulty(secretManager);
      faulty.writtenVersions.clear();

      alter(secretManager, props, bind(KEY, "faulty", "new"));
      Assertions.assertEquals(1, faulty.switches);
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      Assertions.assertEquals(faulty.writtenVersions, faulty.deletedVersions);
    }
  }

  @Test
  void testFailedSwitchKeepsBothVersionsWhenCurrentIsUnknown() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind("unknown", "faulty", "old"));
      String old = props.get("unknown") + "@" + currentVersion(secretManager, props.get("unknown"));
      FaultySecretsProvider faulty = faulty(secretManager);

      alter(secretManager, props, bind("unknown", "faulty", "new"));
      Assertions.assertEquals(1, faulty.switches);
      Assertions.assertTrue(faulty.deletedVersions.isEmpty());
      Assertions.assertEquals("old", read(secretManager, props, "unknown"));

      // Clear the lookup failure so the remove releases the old version. That leaves the URN with
      // the new version and no current one; binding the key again must still make the rebound
      // value readable.
      faulty.lookupFails = false;
      Map<String, String> removed =
          alter(secretManager, props, CatalogChange.removeProperty("unknown"));
      Assertions.assertEquals(List.of(old), faulty.deletedVersions);
      Assertions.assertNull(currentVersion(secretManager, props.get("unknown")));
      alter(secretManager, removed, bind("unknown", "faulty", "again"));
      Assertions.assertEquals("again", read(secretManager, props, "unknown"));
    }
  }

  @Test
  void testFailedSwitchKeepsAVersionAnotherWriterMadeCurrent() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind("raced", "faulty", "old"));
      FaultySecretsProvider faulty = faulty(secretManager);
      faulty.writtenVersions.clear();

      alter(secretManager, props, bind("raced", "faulty", "new"));
      Assertions.assertEquals(faulty.writtenVersions, faulty.deletedVersions);
      Assertions.assertEquals("other", read(secretManager, props, "raced"));
    }
  }

  @Test
  void testSwitchThatTookEffectBeforeFailingDeletesThePreviousVersion() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind("applied", "faulty", "old"));
      String previous =
          props.get("applied") + "@" + currentVersion(secretManager, props.get("applied"));
      FaultySecretsProvider faulty = faulty(secretManager);

      alter(secretManager, props, bind("applied", "faulty", "new"));
      Assertions.assertEquals("new", read(secretManager, props, "applied"));
      Assertions.assertEquals(List.of(previous), faulty.deletedVersions);
    }
  }

  @Test
  void testFirstBindingCommitsWithoutSwitchOrDelete() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "faulty", "first"));

      FaultySecretsProvider faulty = faulty(secretManager);
      Assertions.assertEquals(0, faulty.switches);
      Assertions.assertTrue(faulty.deletedVersions.isEmpty());
      Assertions.assertEquals("first", read(secretManager, props, KEY));
    }
  }

  @Test
  void testFailedVersionWriteDeletesVersionsWrittenBefore() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "faulty", "old"));
      FaultySecretsProvider faulty = faulty(secretManager);
      faulty.writtenVersions.clear();

      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              prepare(
                  secretManager,
                  props,
                  bind("first", "faulty", "a"),
                  bind(KEY, "faulty", "new"),
                  bind("second", "faulty", "b"),
                  bind("broken", "faulty", "c")));
      Assertions.assertEquals(3, faulty.writtenVersions.size());
      Assertions.assertEquals(
          new HashSet<>(faulty.writtenVersions), new HashSet<>(faulty.deletedVersions));
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      Assertions.assertNull(currentVersion(secretManager, writeThroughUrn("faulty", "first")));
      Assertions.assertNull(currentVersion(secretManager, writeThroughUrn("faulty", "second")));
    }
  }

  @Test
  void testUnversionedProviderFailsEveryAlterOfItsSecrets() {
    try (SecretManager secretManager = secretManager()) {
      SecretUrn urn =
          secretManager
              .getRegistry()
              .getProvider("unversioned")
              .writeSecret("old", writeThroughAttributes(KEY));
      Map<String, String> props = Map.of(KEY, urn.toString());
      CatalogChange[][] batches = {
        {bind(KEY, "unversioned", "new")},
        {CatalogChange.setProperty(KEY, "new")},
        {CatalogChange.removeProperty(KEY)},
        {
          CatalogChange.setSecretReference(
              KEY, new SecretReference("reference", Map.of("path", "kv")))
        },
        {bind(KEY, "memory", "new")},
      };
      for (CatalogChange[] batch : batches) {
        UnsupportedOperationException error =
            Assertions.assertThrows(
                UnsupportedOperationException.class, () -> prepare(secretManager, props, batch));
        Assertions.assertTrue(error.getMessage().contains("\"unversioned\""), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains(KEY), error.getMessage());
      }
      Assertions.assertEquals("old", secretManager.readSecret(urn));
      Assertions.assertNull(currentVersion(secretManager, writeThroughUrn("memory", KEY)));
    }
  }

  @Test
  void testUnversionedProviderLaterInBatchDeletesVersionsWrittenBefore() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "faulty", "old"));
      FaultySecretsProvider faulty = faulty(secretManager);
      faulty.writtenVersions.clear();

      Assertions.assertThrows(
          UnsupportedOperationException.class,
          () ->
              prepare(
                  secretManager,
                  props,
                  bind(KEY, "faulty", "new"),
                  bind("k2", "unversioned", "x")));
      Assertions.assertEquals(1, faulty.writtenVersions.size());
      Assertions.assertEquals(faulty.writtenVersions, faulty.deletedVersions);
      Assertions.assertEquals("old", read(secretManager, props, KEY));
    }
  }

  @Test
  void testRemoveKeepsExternalReference() {
    try (SecretManager secretManager = secretManager()) {
      // External-ref URNs are not entityType:entityId:propertyKey write-through shapes.
      String externalUrn = "urn:gravitino-secret:memory:path:kv/jdbc-password:jdbc-password";
      Assertions.assertFalse(
          SecretPropertyUtils.isWriteThroughForEntity(KEY, externalUrn, "catalog", ID));

      SecretVersionChanges prepared =
          prepare(secretManager, Map.of(KEY, externalUrn), CatalogChange.removeProperty(KEY))
              .getRight();
      Assertions.assertTrue(prepared.written().isEmpty());
      Assertions.assertTrue(prepared.released().isEmpty());
    }
  }

  @Test
  void testSchemaAndFilesetSecretsUseTheirEntityType() {
    try (SecretManager secretManager = secretManager()) {
      Pair<SchemaChange[], SecretVersionChanges> schema =
          SecretAlterChanges.prepareSchemaChanges(
              secretManager,
              null,
              ID,
              SchemaChange.setSecretBinding("k2", new SecretBinding("memory", "s")));
      Pair<FilesetChange[], SecretVersionChanges> fileset =
          SecretAlterChanges.prepareFilesetChanges(
              secretManager,
              null,
              ID,
              FilesetChange.setSecretBinding("k2", new SecretBinding("memory", "f")));

      SecretUrn schemaUrn = schema.getRight().written().get(0).version().urn();
      SecretUrn filesetUrn = fileset.getRight().written().get(0).version().urn();
      Assertions.assertEquals("schema", schemaUrn.identifierSegments().get(0));
      Assertions.assertEquals("fileset", filesetUrn.identifierSegments().get(0));
      Assertions.assertEquals("s", secretManager.readSecret(schemaUrn));
      Assertions.assertEquals("f", secretManager.readSecret(filesetUrn));

      secretManager.rollbackSecrets(schema.getRight());
      secretManager.rollbackSecrets(fileset.getRight());
      Assertions.assertNull(currentVersion(secretManager, schemaUrn.toString()));
      Assertions.assertNull(currentVersion(secretManager, filesetUrn.toString()));
    }
  }

  @Test
  void testEntityWithoutIdRejectsWriteThroughChanges() {
    try (SecretManager secretManager = secretManager()) {
      SecretBinding binding = new SecretBinding("memory", "s");
      SecretReference reference = new SecretReference("reference", Map.of("path", "kv"));
      String message =
          assertNoEntityId(
              () ->
                  SecretAlterChanges.prepareSchemaChanges(
                      secretManager, Map.of(), null, SchemaChange.setSecretBinding("k", binding)));
      Assertions.assertTrue(message.contains("load the schema to import it first"));
      // A reference earlier in the batch does not let the binding through.
      assertNoEntityId(
          () ->
              SecretAlterChanges.prepareFilesetChanges(
                  secretManager,
                  Map.of(),
                  null,
                  FilesetChange.setSecretReference("a", reference),
                  FilesetChange.setSecretBinding("k", binding)));

      // A write-through URN already on the entity cannot be replaced, rewritten or removed.
      String schemaUrn = "urn:gravitino-secret:memory:schema:5:" + KEY;
      for (SchemaChange change :
          new SchemaChange[] {
            SchemaChange.setSecretReference(KEY, reference),
            SchemaChange.removeProperty(KEY),
            SchemaChange.setProperty(KEY, "new")
          }) {
        assertNoEntityId(
            () ->
                SecretAlterChanges.prepareSchemaChanges(
                    secretManager, Map.of(KEY, schemaUrn), null, change));
      }
      String filesetUrn = "urn:gravitino-secret:memory:fileset:5:" + KEY;
      for (FilesetChange change :
          new FilesetChange[] {
            FilesetChange.setSecretReference(KEY, reference),
            FilesetChange.removeProperty(KEY),
            FilesetChange.setProperty(KEY, "new")
          }) {
        assertNoEntityId(
            () ->
                SecretAlterChanges.prepareFilesetChanges(
                    secretManager, Map.of(KEY, filesetUrn), null, change));
      }
      // The rejected setProperty did not rewrite either secret.
      Assertions.assertNull(currentVersion(secretManager, schemaUrn));
      Assertions.assertNull(currentVersion(secretManager, filesetUrn));
    }
  }

  @Test
  void testEntityWithoutIdAcceptsReferencesAndPlainProperties() {
    try (SecretManager secretManager = secretManager()) {
      SecretReference reference = new SecretReference("reference", Map.of("path", "kv"));
      Pair<FilesetChange[], SecretVersionChanges> result =
          SecretAlterChanges.prepareFilesetChanges(
              secretManager,
              Map.of("plain", "a"),
              null,
              FilesetChange.setSecretReference(KEY, reference),
              FilesetChange.setProperty("plain", "b"),
              FilesetChange.removeProperty("other"));

      Assertions.assertSame(SecretVersionChanges.EMPTY, result.getRight());
      FilesetChange.SetProperty referenced = (FilesetChange.SetProperty) result.getLeft()[0];
      Assertions.assertEquals("urn:gravitino-secret:reference:kv:" + KEY, referenced.getValue());
      Assertions.assertEquals("b", ((FilesetChange.SetProperty) result.getLeft()[1]).getValue());

      // Other secret URNs on the key can still be replaced or removed: only a write-through URN of
      // three segments, this entity type, a numeric id and this key needs the id. Each of the last
      // four fails exactly one of those conditions.
      for (String current :
          new String[] {
            "urn:gravitino-secret:reference:kv:" + KEY,
            "urn:gravitino-secret:reference:schema:5:" + KEY + ":" + KEY,
            "urn:gravitino-secret:memory:fileset:5:" + KEY,
            "urn:gravitino-secret:reference:schema:app:" + KEY,
            "urn:gravitino-secret:reference:schema:5:db-" + KEY
          }) {
        Map<String, String> props = Map.of(KEY, current);
        SecretAlterChanges.prepareSchemaChanges(
            secretManager, props, null, SchemaChange.removeProperty(KEY));
        SecretAlterChanges.prepareSchemaChanges(
            secretManager, props, null, SchemaChange.setSecretReference(KEY, reference));
      }
    }
  }

  @Test
  void testRejectedBatchWritesNothing() {
    try (SecretManager secretManager = secretManager()) {
      Map<String, String> props = alter(secretManager, Map.of(), bind(KEY, "memory", "old"));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> prepare(secretManager, props, CatalogChange.setProperty(KEY, "******")));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              prepare(
                  secretManager,
                  props,
                  bind("first", "memory", "a"),
                  CatalogChange.setProperty(
                      "other", "urn:gravitino-secret:memory:catalog:1:other")));
      Assertions.assertEquals("old", read(secretManager, props, KEY));
      Assertions.assertNull(currentVersion(secretManager, writeThroughUrn("memory", "first")));
    }
  }

  /** Asserts that {@code prepare} is rejected for lack of an entity id and returns the message. */
  private static String assertNoEntityId(Executable prepare) {
    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, prepare);
    Assertions.assertTrue(
        error.getMessage().contains("no Gravitino entity id"), error.getMessage());
    return error.getMessage();
  }

  private static CatalogChange bind(String key, String provider, String plaintext) {
    return CatalogChange.setSecretBinding(key, new SecretBinding(provider, plaintext));
  }

  private static Pair<CatalogChange[], SecretVersionChanges> prepare(
      SecretManager secretManager, Map<String, String> props, CatalogChange... changes) {
    return SecretAlterChanges.prepareCatalogChanges(secretManager, props, ID, changes);
  }

  /** Prepares and commits {@code changes}, returning the properties the entity would store. */
  private static Map<String, String> alter(
      SecretManager secretManager, Map<String, String> props, CatalogChange... changes) {
    Pair<CatalogChange[], SecretVersionChanges> result = prepare(secretManager, props, changes);
    secretManager.commitSecrets(result.getRight());
    return applied(props, result.getLeft());
  }

  private static Map<String, String> applied(
      Map<String, String> props, CatalogChange... effectiveChanges) {
    Map<String, String> result = new HashMap<>(props);
    for (CatalogChange change : effectiveChanges) {
      if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty set = (CatalogChange.SetProperty) change;
        result.put(set.getProperty(), set.getValue());
      } else if (change instanceof CatalogChange.RemoveProperty) {
        result.remove(((CatalogChange.RemoveProperty) change).getProperty());
      }
    }
    return result;
  }

  private static SecretUrn urn(Map<String, String> props, String key) {
    return SecretUrn.parse(props.get(key));
  }

  private static String read(SecretManager secretManager, Map<String, String> props, String key) {
    return secretManager.readSecret(urn(props, key));
  }

  private static String currentVersion(SecretManager secretManager, String urn) {
    SecretUrn parsed = SecretUrn.parse(urn);
    return secretManager
        .getRegistry()
        .getProvider(parsed.providerName())
        .currentSecretVersion(parsed);
  }

  private static String writeThroughUrn(String provider, String key) {
    return SecretUrn.buildWriteThrough(provider, writeThroughAttributes(key)).toString();
  }

  private static Map<String, String> writeThroughAttributes(String key) {
    return Map.of(
        SecretConstants.ATTR_ENTITY_TYPE,
        "catalog",
        SecretConstants.ATTR_ENTITY_ID,
        String.valueOf(ID),
        SecretConstants.ATTR_PROPERTY_KEY,
        key);
  }

  private static FaultySecretsProvider faulty(SecretManager secretManager) {
    return (FaultySecretsProvider) secretManager.getRegistry().getProvider("faulty");
  }

  private static SecretProvider provider(SecretManager secretManager) {
    return secretManager.getRegistry().getProvider("memory");
  }

  private static SecretManager secretManager() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS,
        "memory,memory2,faulty,unversioned,reference");
    String prefix = SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX;
    String className = SecretProviderRegistry.CLASS_NAME;
    properties.setProperty(prefix + "memory." + className, InMemorySecretsProvider.class.getName());
    properties.setProperty(
        prefix + "memory2." + className, InMemorySecretsProvider.class.getName());
    properties.setProperty(prefix + "faulty." + className, FaultySecretsProvider.class.getName());
    properties.setProperty(
        prefix + "unversioned." + className, UnversionedSecretsProvider.class.getName());
    properties.setProperty(
        prefix + "reference." + className,
        TestCatalogManager.TestReferenceSecretsProvider.class.getName());
    config.loadFromProperties(properties);
    return new SecretManager(config);
  }
}
