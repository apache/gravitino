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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.types.Type;

/**
 * An in-memory {@link FunctionDispatcher} that stores function metadata in memory. This is a
 * temporary implementation intended for wiring REST paths before a persistent backend is added.
 */
public class InMemoryFunctionDispatcher implements FunctionDispatcher {

  private final Map<NameIdentifier, Map<FunctionSignature, NavigableMap<Integer, StoredFunction>>>
      functions = new HashMap<>();

  @Override
  public synchronized NameIdentifier[] listFunctions(Namespace namespace)
      throws NoSuchSchemaException { // NO SONAR: kept for interface compatibility
    return functions.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public synchronized Function[] getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null || bySignature.isEmpty()) {
      throw new NoSuchFunctionException("Function not found: " + ident);
    }
    return latestFunctions(bySignature.values());
  }

  @Override
  public synchronized Function[] getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    Map<FunctionSignature, NavigableMap<Integer, StoredFunction>> bySignature =
        functions.get(ident);
    if (bySignature == null || bySignature.isEmpty()) {
      throw new NoSuchFunctionException("Function not found: " + ident);
    }

    Function[] matched =
        bySignature.values().stream()
            .map(versions -> versions.get(version))
            .filter(Objects::nonNull)
            .toArray(Function[]::new);
    if (matched.length == 0) {
      throw new NoSuchFunctionVersionException(
          "Function version " + version + " not found for " + ident);
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
      throw new NoSuchFunctionException("Function not found: " + ident);
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
        latest.newVersion(latest.version() + 1, comment, impls, latest.auditInfo());
    versions.put(updated.version(), updated);
    return updated;
  }

  @Override
  public synchronized boolean deleteFunction(NameIdentifier ident) {
    return functions.remove(ident) != null;
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
          "Function already exists with signature: " + signature);
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
            AuditInfo.EMPTY);
    NavigableMap<Integer, StoredFunction> versions = new TreeMap<>();
    versions.put(stored.version(), stored);
    bySignature.put(signature, versions);
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
      throw new NoSuchFunctionException("Function not found for signature: " + target);
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
