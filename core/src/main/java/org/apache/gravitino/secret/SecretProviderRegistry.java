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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and owns named {@link SecretProvider} instances from server configuration for the process
 * lifetime (owned by {@code GravitinoEnv}, analogous to the KMS client registry).
 *
 * <p>Configuration shape:
 *
 * <pre>{@code
 * gravitino.secret.providers=memory
 * gravitino.secret.provider.memory.className=org.apache.gravitino.secret.memory.InMemorySecretsProvider
 * gravitino.secret.provider.<name>.uri=<optional-non-secret-endpoint>
 * }</pre>
 */
public final class SecretProviderRegistry implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SecretProviderRegistry.class);

  /** Comma-separated configured provider instance names. */
  public static final String GRAVITINO_SECRET_PROVIDERS = "gravitino.secret.providers";

  /** Prefix for per-provider settings: {@code gravitino.secret.provider.<name>.}. */
  public static final String GRAVITINO_SECRET_PROVIDER_PREFIX = "gravitino.secret.provider.";

  /** Fully qualified {@link SecretProvider} implementation class. */
  public static final String CLASS_NAME = "className";

  /** Optional non-secret provider endpoint exposed by the discovery API. */
  public static final String URI = "uri";

  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static final Pattern PROVIDER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");

  private final Map<String, SecretProvider> providers;
  private final List<SecretProviderInfo> providerInfos;
  private volatile boolean closed;

  /**
   * Creates a registry from Gravitino configuration.
   *
   * @param config Gravitino configuration; must not be {@code null}
   * @throws IllegalArgumentException if configuration is invalid
   * @throws IllegalStateException if a provider cannot be loaded or initialized
   */
  public SecretProviderRegistry(Config config) {
    Preconditions.checkArgument(config != null, "config must not be null");
    Map<String, SecretProvider> loaded = new LinkedHashMap<>();
    List<SecretProviderInfo> infos = new ArrayList<>();
    try {
      for (String name : parseProviderNames(config.getRawString(GRAVITINO_SECRET_PROVIDERS, ""))) {
        LoadedProvider loadedProvider = loadProvider(name, config);
        loaded.put(name, loadedProvider.provider);
        infos.add(loadedProvider.info);
      }
    } catch (RuntimeException e) {
      closeQuietly(loaded.values());
      throw e;
    }
    this.providers = ImmutableMap.copyOf(loaded);
    this.providerInfos = ImmutableList.copyOf(infos);
  }

  /**
   * Returns safe metadata for all configured providers.
   *
   * @return an immutable list of provider metadata (empty when none are configured)
   */
  public List<SecretProviderInfo> listProviders() {
    checkOpen();
    return providerInfos;
  }

  /**
   * Returns the live provider registered under {@code name}.
   *
   * @param name the configured provider instance name
   * @return the provider
   * @throws IllegalArgumentException if the name is unknown
   * @throws IllegalStateException if the registry is closed
   */
  public SecretProvider getProvider(String name) {
    checkOpen();
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    SecretProvider provider = providers.get(name);
    Preconditions.checkArgument(provider != null, "Unknown secret provider '%s'", name);
    return provider;
  }

  /**
   * Returns whether a provider is registered under {@code name}.
   *
   * @param name the configured provider instance name
   * @return {@code true} if the provider exists
   */
  public boolean contains(String name) {
    checkOpen();
    return providers.containsKey(name);
  }

  /** Closes all configured providers. This operation is idempotent. */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    closeQuietly(providers.values());
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("SecretProviderRegistry is closed");
    }
  }

  private static ImmutableList<String> parseProviderNames(String value) {
    if (StringUtils.isBlank(value)) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> names = ImmutableList.builder();
    Set<String> unique = new LinkedHashSet<>();
    for (String name : COMMA_SPLITTER.split(value)) {
      if (!PROVIDER_NAME_PATTERN.matcher(name).matches()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid secret provider name '%s' in %s", name, GRAVITINO_SECRET_PROVIDERS));
      }
      if (!unique.add(name)) {
        throw new IllegalArgumentException(
            String.format(
                "Duplicate secret provider name '%s' in %s", name, GRAVITINO_SECRET_PROVIDERS));
      }
      names.add(name);
    }
    return names.build();
  }

  private static LoadedProvider loadProvider(String name, Config config) {
    String prefix = GRAVITINO_SECRET_PROVIDER_PREFIX + name + ".";
    Map<String, String> properties = MapUtils.getPrefixMap(config.getAllConfig(), prefix);
    String className = properties.get(CLASS_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(className),
        "Secret provider '%s' is missing required config %s%s",
        name,
        prefix,
        CLASS_NAME);

    Map<String, String> providerConfig = new LinkedHashMap<>(properties);
    providerConfig.remove(CLASS_NAME);
    String uri = blankToNull(providerConfig.get(URI));

    SecretProvider provider = instantiate(name, className);
    try {
      provider.initialize(name, ImmutableMap.copyOf(providerConfig));
    } catch (RuntimeException e) {
      closeQuietly(ImmutableList.of(provider));
      throw new IllegalStateException(
          String.format("Failed to initialize secret provider '%s' (%s)", name, className), e);
    }

    String type = provider.type();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type),
        "Secret provider '%s' (%s) returned a blank type()",
        name,
        className);
    LOG.info("Loaded secret provider '{}' of type '{}'", name, type);
    return new LoadedProvider(provider, new SecretProviderInfo(name, type, uri));
  }

  private static SecretProvider instantiate(String name, String className) {
    try {
      Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
      Preconditions.checkArgument(
          instance instanceof SecretProvider,
          "Secret provider '%s' className '%s' does not implement SecretProvider",
          name,
          className);
      return (SecretProvider) instance;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          String.format("Failed to load secret provider '%s' (%s)", name, className), e);
    }
  }

  private static void closeQuietly(Iterable<SecretProvider> providers) {
    for (SecretProvider provider : providers) {
      try {
        provider.close();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close secret provider", e);
      }
    }
  }

  private static String blankToNull(String value) {
    return StringUtils.isBlank(value) ? null : value;
  }

  private static final class LoadedProvider {
    private final SecretProvider provider;
    private final SecretProviderInfo info;

    private LoadedProvider(SecretProvider provider, SecretProviderInfo info) {
      this.provider = provider;
      this.info = info;
    }
  }
}
