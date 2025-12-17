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
package org.apache.gravitino.catalog;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.function.FunctionDTO;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * A file-backed {@link FunctionDispatcher} that caches function metadata in memory and persists it
 * under {@code ${GRAVITINO_HOME}/data/functions}.
 */
public class FileBackedFunctionDispatcher implements FunctionDispatcher {

  private static final String FUNCTIONS_DATA_DIR = "data";
  private static final String FUNCTIONS_DIR_NAME = "functions";
  private static final String FUNCTIONS_FILE_NAME = "functions.json";

  private final Map<NameIdentifier, Map<FunctionSignature, NavigableMap<Integer, StoredFunction>>>
      functions = new HashMap<>();
  private final Path storageFile;

  public FileBackedFunctionDispatcher() {
    Path storageDir = resolveStorageDir();
    if (Files.exists(storageDir) && !Files.isDirectory(storageDir)) {
      throw new IllegalStateException(
          String.format("Functions storage path is not a directory: %s", storageDir));
    }
    try {
      Files.createDirectories(storageDir);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Failed to create functions storage directory: %s", storageDir), e);
    }
    this.storageFile = storageDir.resolve(FUNCTIONS_FILE_NAME);
    loadFromDisk();
  }

  @Override
  public synchronized NameIdentifier[] listFunctions(Namespace namespace)
      throws NoSuchSchemaException { // NO SONAR: kept for interface compatibility
    return functions.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public synchronized Function[] listFunctionInfos(Namespace namespace)
      throws NoSuchSchemaException { // NO SONAR: kept for interface compatibility
    return functions.entrySet().stream()
        .filter(entry -> entry.getKey().namespace().equals(namespace))
        .flatMap(entry -> Arrays.stream(latestFunctions(entry.getValue().values())))
        .toArray(Function[]::new);
  }

  @Override
  public synchronized Function[] getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null || bySignature.isEmpty()) {
      throw new NoSuchFunctionException("Function not found: %s", ident);
    }
    return latestFunctions(bySignature.values());
  }

  @Override
  public synchronized Function[] getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null || bySignature.isEmpty()) {
      throw new NoSuchFunctionException("Function not found: %s", ident);
    }

    Function[] matched =
        bySignature.values().stream()
            .map(versions -> versions.get(version))
            .filter(Objects::nonNull)
            .toArray(Function[]::new);
    if (matched.length == 0) {
      throw new NoSuchFunctionVersionException(
          "Function version %s not found for %s", version, ident);
    }
    return matched;
  }

  @Override
  public synchronized Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      FunctionParam[] functionParams,
      Type returnType,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException { // NOSONAR
    FunctionSignature signature = FunctionSignature.of(ident.name(), functionParams);
    return registerInternal(
        ident,
        signature,
        functionType,
        deterministic,
        comment,
        returnType,
        new FunctionColumn[0],
        functionImpls);
  }

  @Override
  public synchronized Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionParam[] functionParams,
      FunctionColumn[] returnColumns,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException { // NOSONAR
    FunctionSignature signature = FunctionSignature.of(ident.name(), functionParams);
    return registerInternal(
        ident,
        signature,
        FunctionType.TABLE,
        deterministic,
        comment,
        null,
        returnColumns,
        functionImpls);
  }

  @Override
  public synchronized Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    if (changes == null || changes.length == 0) {
      throw new IllegalArgumentException("At least one change must be provided");
    }

    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null || bySignature.isEmpty()) {
      throw new NoSuchFunctionException("Function not found: %s", ident);
    }

    FunctionSignature targetSignature = resolveTargetSignature(bySignature, changes);
    NavigableMap<Integer, StoredFunction> versions = bySignature.get(targetSignature);
    StoredFunction latest = versions.lastEntry().getValue();

    String comment = latest.comment();
    FunctionImpl[] impls = latest.impls();

    for (FunctionChange change : changes) {
      if (change instanceof FunctionChange.UpdateComment) {
        comment = ((FunctionChange.UpdateComment) change).newComment();
      } else if (change instanceof FunctionChange.UpdateImplementations) {
        impls = ((FunctionChange.UpdateImplementations) change).newImplementations();
      } else if (change instanceof FunctionChange.AddImplementation) {
        FunctionImpl implementation = ((FunctionChange.AddImplementation) change).implementation();
        FunctionImpl[] newImpls = Arrays.copyOf(impls, impls.length + 1);
        newImpls[newImpls.length - 1] = implementation;
        impls = newImpls;
      } else {
        throw new IllegalArgumentException("Unsupported function change: " + change);
      }
    }

    StoredFunction updated =
        latest.newVersion(
            latest.version() + 1,
            comment,
            impls,
            AuditInfo.builder()
                .withCreator(latest.auditInfo().creator())
                .withCreateTime(latest.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build());
    versions.put(updated.version(), updated);
    persistToDisk();
    return updated;
  }

  @Override
  public synchronized boolean deleteFunction(NameIdentifier ident) {
    boolean removed = functions.remove(ident) != null;
    if (removed) {
      persistToDisk();
    }
    return removed;
  }

  @Override
  public synchronized boolean deleteFunction(NameIdentifier ident, FunctionSignature signature) {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null) {
      return false;
    }
    boolean removed = bySignature.remove(signature) != null;
    if (bySignature.isEmpty()) {
      functions.remove(ident);
    }
    if (removed) {
      persistToDisk();
    }
    return removed;
  }

  private Function registerInternal(
      NameIdentifier ident,
      FunctionSignature signature,
      FunctionType functionType,
      boolean deterministic,
      String comment,
      Type returnType,
      FunctionColumn[] returnColumns,
      FunctionImpl[] functionImpls)
      throws FunctionAlreadyExistsException {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.computeIfAbsent(ident, ignored -> new HashMap<>());
    if (bySignature.containsKey(signature)) {
      throw new FunctionAlreadyExistsException(
          "Function already exists with signature: %s", signature);
    }

    StoredFunction stored =
        new StoredFunction(
            signature,
            functionType,
            deterministic,
            comment,
            returnType,
            returnColumns,
            functionImpls,
            1,
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build());
    NavigableMap<Integer, StoredFunction> versions = new TreeMap<>();
    versions.put(stored.version(), stored);
    bySignature.put(signature, versions);
    persistToDisk();
    return stored;
  }

  private FunctionSignature resolveTargetSignature(
      Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature,
      FunctionChange[] changes)
      throws NoSuchFunctionException {
    FunctionSignature target = null;
    for (FunctionChange change : changes) {
      FunctionSignature changeSig = change.signature();
      if (changeSig != null) {
        if (target != null && !target.equals(changeSig)) {
          throw new IllegalArgumentException("All changes must target the same signature");
        }
        target = changeSig;
      }
    }

    if (target == null) {
      if (bySignature.size() != 1) {
        throw new IllegalArgumentException(
            "Signature must be provided when multiple overloads exist");
      }
      target = bySignature.keySet().iterator().next();
    }

    if (!bySignature.containsKey(target)) {
      throw new NoSuchFunctionException("Function not found for signature: %s", target);
    }

    return target;
  }

  private Function[] latestFunctions(
      Collection<NavigableMap<Integer, StoredFunction>> versionedFunctions) {
    return versionedFunctions.stream()
        .map(NavigableMap::lastEntry)
        .map(Map.Entry::getValue)
        .toArray(Function[]::new);
  }

  private Path resolveStorageDir() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    boolean isTest = Boolean.parseBoolean(System.getenv("GRAVITINO_TEST"));
    if (isTest) {
      String itProjectDir = System.getenv("IT_PROJECT_DIR");
      if (StringUtils.isNotBlank(itProjectDir)) {
        return Paths.get(itProjectDir, FUNCTIONS_DATA_DIR, FUNCTIONS_DIR_NAME);
      }
      return Paths.get(
          System.getProperty("java.io.tmpdir"),
          "gravitino-test",
          FUNCTIONS_DATA_DIR,
          FUNCTIONS_DIR_NAME);
    }
    Preconditions.checkArgument(StringUtils.isNotBlank(gravitinoHome), "GRAVITINO_HOME not set");
    return Paths.get(gravitinoHome, FUNCTIONS_DATA_DIR, FUNCTIONS_DIR_NAME);
  }

  private void loadFromDisk() {
    if (!Files.exists(storageFile)) {
      return;
    }
    try (Reader reader = Files.newBufferedReader(storageFile, StandardCharsets.UTF_8)) {
      PersistedFunctionRecord[] records =
          JsonUtils.objectMapper().readValue(reader, PersistedFunctionRecord[].class);
      functions.clear();
      for (PersistedFunctionRecord record : records) {
        if (record == null || record.function == null || record.identifier == null) {
          continue;
        }
        NameIdentifier identifier = NameIdentifier.parse(record.identifier);
        StoredFunction stored = toStoredFunction(record.function);
        Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
            functions.computeIfAbsent(identifier, ignored -> new HashMap<>());
        NavigableMap<Integer, StoredFunction> versions =
            bySignature.computeIfAbsent(stored.signature(), ignored -> new TreeMap<>());
        versions.put(stored.version(), stored);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Failed to load function metadata from %s", storageFile), e);
    }
  }

  private void persistToDisk() {
    List<PersistedFunctionRecord> records = new ArrayList<>();
    for (Map.Entry<NameIdentifier, Map<FunctionSignature, NavigableMap<Integer, StoredFunction>>>
        entry : functions.entrySet()) {
      String identifier = entry.getKey().toString();
      for (NavigableMap<Integer, StoredFunction> versions : entry.getValue().values()) {
        for (StoredFunction function : versions.values()) {
          records.add(new PersistedFunctionRecord(identifier, FunctionDTO.fromFunction(function)));
        }
      }
    }
    try {
      Files.createDirectories(storageFile.getParent());
      Path tempFile =
          Files.createTempFile(storageFile.getParent(), FUNCTIONS_DIR_NAME, ".json.tmp");
      try (Writer writer =
          Files.newBufferedWriter(
              tempFile,
              StandardCharsets.UTF_8,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        JsonUtils.objectMapper().writeValue(writer, records);
      }
      try {
        Files.move(
            tempFile,
            storageFile,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tempFile, storageFile, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Failed to persist function metadata to %s", storageFile), e);
    }
  }

  private StoredFunction toStoredFunction(Function function) {
    return new StoredFunction(
        function.signature(),
        function.functionType(),
        function.deterministic(),
        function.comment(),
        function.returnType(),
        function.returnColumns(),
        function.impls(),
        function.version(),
        function.auditInfo());
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private static final class PersistedFunctionRecord {
    private String identifier;
    private FunctionDTO function;

    PersistedFunctionRecord() {}

    PersistedFunctionRecord(String identifier, FunctionDTO function) {
      this.identifier = identifier;
      this.function = function;
    }
  }

  private static final class StoredFunction implements Function {
    private final FunctionSignature signature;
    private final FunctionType functionType;
    private final boolean deterministic;
    private final String comment;
    private final Type returnType;
    private final FunctionColumn[] returnColumns;
    private final FunctionImpl[] impls;
    private final int version;
    private final Audit audit;

    StoredFunction(
        FunctionSignature signature,
        FunctionType functionType,
        boolean deterministic,
        String comment,
        Type returnType,
        FunctionColumn[] returnColumns,
        FunctionImpl[] impls,
        int version,
        Audit audit) {
      this.signature = signature;
      this.functionType = functionType;
      this.deterministic = deterministic;
      this.comment = comment;
      this.returnType = returnType;
      this.returnColumns =
          returnColumns == null
              ? new FunctionColumn[0]
              : Arrays.copyOf(returnColumns, returnColumns.length);
      this.impls = impls == null ? new FunctionImpl[0] : Arrays.copyOf(impls, impls.length);
      this.version = version;
      this.audit = audit;
    }

    @Override
    public FunctionSignature signature() {
      return signature;
    }

    @Override
    public FunctionType functionType() {
      return functionType;
    }

    @Override
    public boolean deterministic() {
      return deterministic;
    }

    @Override
    public String comment() {
      return comment;
    }

    @Override
    public Type returnType() {
      return returnType;
    }

    @Override
    public FunctionImpl[] impls() {
      return Arrays.copyOf(impls, impls.length);
    }

    @Override
    public FunctionColumn[] returnColumns() {
      return Arrays.copyOf(returnColumns, returnColumns.length);
    }

    @Override
    public Audit auditInfo() {
      return audit;
    }

    @Override
    public int version() {
      return version;
    }

    StoredFunction newVersion(
        int newVersion, String newComment, FunctionImpl[] newImpls, Audit newAudit) {
      return new StoredFunction(
          signature,
          functionType,
          deterministic,
          newComment,
          returnType,
          returnColumns,
          newImpls,
          newVersion,
          newAudit == null ? audit : newAudit);
    }
  }
}
