/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.connector.credential;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Generates {@link PathBasedCredentialContext}, the catalog operation should implement this
 * interface to generate the path based credentials.
 */
public interface SupportsPathBasedCredentials {
  /**
   * The object contains necessary information to generate {@link PathBasedCredentialContext}.
   *
   * <p>The objects like {@link org.apache.gravitino.file.Fileset}, ${@link
   * org.apache.gravitino.rel.Table} Should generate this object to void load multipal times to get
   * paths and properties.
   */
  class PathBasedCredentialContextInfo {
    private List<String> paths;
    private Map<String, String> properties;

    /**
     * {@link PathBasedCredentialContextInfo} constructor.
     *
     * @param paths The paths of the object, used to generate {@link
     *     PathBasedCredentialContextInfo}.
     * @param properties The properties of the object, used to get credential provider
     *     configurations.
     */
    public PathBasedCredentialContextInfo(List<String> paths, Map<String, String> properties) {
      this.paths = paths;
      this.properties = properties;
    }
  }

  /**
   * The schema properties.
   *
   * <p>Used to get credential provider configuration from schema if necessary.
   *
   * @param schemaIdentifier The schema identifier.
   * @return The schema properties.
   */
  Map<String, String> schemaProperties(NameIdentifier schemaIdentifier);

  /**
   * Get {@link PathBasedCredentialContextInfo}.
   *
   * @param nameIdentifier, The identifier for fileset, table, etc.
   * @return The {@link PathBasedCredentialContextInfo} instance.
   */
  PathBasedCredentialContextInfo getPathBasedCredentialContextInfo(NameIdentifier nameIdentifier);

  /**
   * Get the mapping of credential provider type and {@link CredentialContext}.
   *
   * <p>In most cases, there will be only one element in the map, For fileset catalog which supports
   * multiple credentials, there will be multiple elements.
   *
   * <p>The default implement only supports one path in one {@link PathBasedCredentialContextInfo}.
   *
   * @param nameIdentifier The name identifier for the fileset, table, etc.
   * @param privilege The credential privilege object.
   * @param catalogProperties The catalog properties, used to get credential provider configuration.
   * @return A map with credential provider type and {@link CredentialContext}.
   */
  default Map<String, CredentialContext> getPathBasedCredentialContexts(
      NameIdentifier nameIdentifier,
      CredentialPrivilege privilege,
      Map<String, String> catalogProperties) {

    PathBasedCredentialContextInfo credentialObject =
        getPathBasedCredentialContextInfo(nameIdentifier);

    List<String> paths = credentialObject.paths;
    Preconditions.checkArgument(
        paths.size() == 1, "Only support one path in one credential object");
    Set<String> writePaths = new HashSet<>();
    Set<String> readPaths = new HashSet<>();
    if (CredentialPrivilege.WRITE.equals(privilege)) {
      writePaths.add(paths.get(0));
    } else {
      readPaths.add(paths.get(0));
    }
    PathBasedCredentialContext credentialContext =
        new PathBasedCredentialContext(PrincipalUtils.getCurrentUserName(), writePaths, readPaths);

    Namespace namespace = nameIdentifier.namespace();
    Preconditions.checkArgument(
        namespace.levels().length == 3, "" + "The namespace of fileset should be 3 levels");
    NameIdentifier namespaceIdentifier =
        NameIdentifier.of(namespace.levels()[0], namespace.levels()[1], namespace.levels()[2]);
    Set<String> providers =
        CredentialUtils.getCredentialProviders(
            () -> credentialObject.properties,
            () -> schemaProperties(namespaceIdentifier),
            () -> catalogProperties);

    return providers.stream()
        .collect(Collectors.toMap(provider -> provider, provider -> credentialContext));
  }
}
