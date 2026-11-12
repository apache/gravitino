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

package org.apache.gravitino.spark.connector.plugin.restcatalog;

import static org.apache.gravitino.spark.connector.ConnectorConstants.COMMA;
import static org.apache.gravitino.spark.connector.utils.ConnectorUtil.removeDuplicateSparkExtensions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;

class GravitinoLakehouseRESTDiscoveryDriverPlugin implements DriverPlugin {

  @VisibleForTesting
  static final String REGISTRATION_POLICY_CONFIG = "spark.sql.gravitino.REST.registrationPolicy";

  private static final Logger LOG =
      LoggerFactory.getLogger(GravitinoLakehouseRESTDiscoveryDriverPlugin.class);
  private static final String GRAVITINO_PREFIX = "spark.sql.gravitino.";
  private static final String SPARK_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String URI_SUFFIX = "REST.uri";
  private static final String CATALOG_PROPERTIES_INFIX = "REST.catalogProperties.";
  private static final Pattern PROVIDER_URI_PATTERN =
      Pattern.compile("^spark\\.sql\\.gravitino\\.([A-Za-z][A-Za-z0-9]*)REST\\.uri$");
  // Matches keys that look like a discovery URI but are spelled differently, so a typo such as
  // `spark.sql.gravitino.lanceRest.uri` is reported instead of silently disabling discovery.
  private static final Pattern SIMILAR_URI_PATTERN =
      Pattern.compile("^spark\\.sql\\.gravitino\\..*rest\\.uri$", Pattern.CASE_INSENSITIVE);
  private static final CatalogRegistrationPolicy DEFAULT_POLICY = (format, catalogName) -> true;

  GravitinoLakehouseRESTDiscoveryDriverPlugin() {}

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    initialize(sc.conf());
    return Collections.emptyMap();
  }

  @VisibleForTesting
  void initialize(SparkConf sparkConf) {
    initialize(sparkConf, BuiltinRESTCatalogProviders.providerClassNames());
  }

  @VisibleForTesting
  void initialize(SparkConf sparkConf, Map<String, String> providerClassNames) {
    validatePluginOrder(sparkConf);
    SparkConf userConf = sparkConf.clone();
    Map<String, String> activeFormats = findActiveFormats(userConf);
    if (activeFormats.isEmpty()) {
      warnNoActiveFormat(userConf);
      return;
    }

    ClassLoader classLoader = contextClassLoader();
    CatalogRegistrationPolicy policy = loadRegistrationPolicy(userConf, classLoader);
    List<CatalogRegistration> registrations = new ArrayList<>();
    Set<String> registeredNames = new LinkedHashSet<>();
    Set<String> extensions = new LinkedHashSet<>();

    activeFormats.forEach(
        (format, uri) -> {
          LakehouseRESTCatalogProvider provider =
              loadProvider(format, providerClassNames, classLoader);
          validateProviderRuntime(provider, classLoader);

          Map<String, String> globalProperties = extractCatalogProperties(userConf, format);
          List<String> discoveredCatalogs = listCatalogs(provider, format, uri, globalProperties);
          if (discoveredCatalogs.isEmpty()) {
            LOG.warn("Discovered no {} REST catalog from {}.", format, uri);
          } else {
            LOG.info("Discovered {} REST catalogs {} from {}.", format, discoveredCatalogs, uri);
          }

          for (String catalogName : discoveredCatalogs) {
            addRegistration(
                userConf,
                provider,
                policy,
                format,
                uri,
                catalogName,
                globalProperties,
                registeredNames,
                registrations);
          }
          extensions.addAll(Arrays.asList(provider.sparkExtensions()));
        });

    applyRegistrations(sparkConf, userConf, registrations);
    registerSqlExtensions(sparkConf, extensions);
  }

  private static void validatePluginOrder(SparkConf sparkConf) {
    String configuredPlugins = sparkConf.get("spark.plugins", "");
    List<String> plugins = new ArrayList<>();
    for (String plugin : configuredPlugins.split(COMMA)) {
      if (StringUtils.isNotBlank(plugin)) {
        plugins.add(plugin.trim());
      }
    }

    int discoveryPluginIndex =
        plugins.indexOf(GravitinoLakehouseRESTDiscoveryPlugin.class.getName());
    int gravitinoPluginIndex = plugins.indexOf(GravitinoSparkPlugin.class.getName());
    Preconditions.checkArgument(
        discoveryPluginIndex < 0
            || gravitinoPluginIndex < 0
            || discoveryPluginIndex < gravitinoPluginIndex,
        "%s must be listed before %s in spark.plugins",
        GravitinoLakehouseRESTDiscoveryPlugin.class.getName(),
        GravitinoSparkPlugin.class.getName());
  }

  private static Map<String, String> findActiveFormats(SparkConf userConf) {
    Map<String, String> activeFormats = new TreeMap<>();
    for (Tuple2<String, String> entry : userConf.getAll()) {
      Matcher matcher = PROVIDER_URI_PATTERN.matcher(entry._1);
      if (matcher.matches()) {
        String format = matcher.group(1);
        String uri = entry._2;
        Preconditions.checkArgument(
            StringUtils.isNotBlank(uri),
            "%s%s%s must not be blank",
            GRAVITINO_PREFIX,
            format,
            URI_SUFFIX);
        activeFormats.put(format, uri);
      }
    }
    return activeFormats;
  }

  @VisibleForTesting
  static List<String> similarUriKeys(SparkConf userConf) {
    List<String> similarKeys = new ArrayList<>();
    for (Tuple2<String, String> entry : userConf.getAll()) {
      if (SIMILAR_URI_PATTERN.matcher(entry._1).matches()
          && !PROVIDER_URI_PATTERN.matcher(entry._1).matches()) {
        similarKeys.add(entry._1);
      }
    }
    return similarKeys;
  }

  private static void warnNoActiveFormat(SparkConf userConf) {
    List<String> similarKeys = similarUriKeys(userConf);
    if (similarKeys.isEmpty()) {
      LOG.warn(
          "{} is configured but no {}<format>{} is set, no lakehouse REST catalog is discovered.",
          GravitinoLakehouseRESTDiscoveryPlugin.class.getName(),
          GRAVITINO_PREFIX,
          URI_SUFFIX);
    } else {
      LOG.warn(
          "{} is configured but no {}<format>{} is set, no lakehouse REST catalog is discovered. "
              + "Keys that look misspelled: {}",
          GravitinoLakehouseRESTDiscoveryPlugin.class.getName(),
          GRAVITINO_PREFIX,
          URI_SUFFIX,
          similarKeys);
    }
  }

  private static List<String> listCatalogs(
      LakehouseRESTCatalogProvider provider,
      String format,
      String uri,
      Map<String, String> globalProperties) {
    List<String> discoveredCatalogs;
    try {
      discoveredCatalogs =
          provider.listCatalogs(uri, Collections.unmodifiableMap(globalProperties));
    } catch (RuntimeException | LinkageError e) {
      throw new IllegalStateException(
          String.format("Failed to list %s REST catalogs from %s", format, uri), e);
    }
    Preconditions.checkState(
        discoveredCatalogs != null,
        "Lakehouse REST catalog provider %s returned a null catalog list",
        format);
    return discoveredCatalogs;
  }

  private static CatalogRegistrationPolicy loadRegistrationPolicy(
      SparkConf userConf, ClassLoader classLoader) {
    if (!userConf.contains(REGISTRATION_POLICY_CONFIG)) {
      return DEFAULT_POLICY;
    }

    String policyClassName = userConf.get(REGISTRATION_POLICY_CONFIG);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(policyClassName),
        "%s must not be blank",
        REGISTRATION_POLICY_CONFIG);
    try {
      Class<?> policyClass = Class.forName(policyClassName, true, classLoader);
      Preconditions.checkArgument(
          CatalogRegistrationPolicy.class.isAssignableFrom(policyClass),
          "%s does not implement %s",
          policyClassName,
          CatalogRegistrationPolicy.class.getName());
      return CatalogRegistrationPolicy.class.cast(policyClass.getConstructor().newInstance());
    } catch (ReflectiveOperationException | LinkageError e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate catalog registration policy %s configured by %s",
              policyClassName, REGISTRATION_POLICY_CONFIG),
          e);
    }
  }

  private static LakehouseRESTCatalogProvider loadProvider(
      String format, Map<String, String> providerClassNames, ClassLoader classLoader) {
    String providerClassName = providerClassNames.get(format);
    Preconditions.checkArgument(
        providerClassName != null,
        "No lakehouse REST catalog provider found for configured format: %s",
        format);
    try {
      Class<?> providerClass = Class.forName(providerClassName, true, classLoader);
      Preconditions.checkArgument(
          LakehouseRESTCatalogProvider.class.isAssignableFrom(providerClass),
          "%s does not implement %s",
          providerClassName,
          LakehouseRESTCatalogProvider.class.getName());
      return providerClass
          .asSubclass(LakehouseRESTCatalogProvider.class)
          .getConstructor()
          .newInstance();
    } catch (ReflectiveOperationException | LinkageError e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate lakehouse REST catalog provider %s for format %s",
              providerClassName, format),
          e);
    }
  }

  private static void validateProviderRuntime(
      LakehouseRESTCatalogProvider provider, ClassLoader classLoader) {
    validateRuntimeClass(provider.format(), provider.catalogClassName(), classLoader);
    String[] providerExtensions = provider.sparkExtensions();
    Preconditions.checkState(
        providerExtensions != null,
        "Lakehouse REST catalog provider %s returned null Spark extensions",
        provider.format());
    for (String extension : providerExtensions) {
      validateRuntimeClass(provider.format(), extension, classLoader);
    }
  }

  private static void validateRuntimeClass(
      String format, String className, ClassLoader classLoader) {
    Preconditions.checkState(
        StringUtils.isNotBlank(className),
        "Lakehouse REST catalog provider %s returned a blank runtime class",
        format);
    try {
      Class.forName(className, false, classLoader);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new IllegalArgumentException(
          String.format(
              "Required runtime class %s for lakehouse REST format %s is not available",
              className, format),
          e);
    }
  }

  private static Map<String, String> extractCatalogProperties(SparkConf userConf, String format) {
    String prefix = GRAVITINO_PREFIX + format + CATALOG_PROPERTIES_INFIX;
    Map<String, String> properties = new LinkedHashMap<>();
    for (Tuple2<String, String> entry : userConf.getAllWithPrefix(prefix)) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(entry._1), "%s must include a property name", prefix);
      properties.put(entry._1, entry._2);
    }
    return properties;
  }

  private static void addRegistration(
      SparkConf userConf,
      LakehouseRESTCatalogProvider provider,
      CatalogRegistrationPolicy policy,
      String format,
      String uri,
      String discoveredCatalogName,
      Map<String, String> globalProperties,
      Set<String> registeredNames,
      List<CatalogRegistration> registrations) {
    Preconditions.checkState(
        StringUtils.isNotBlank(discoveredCatalogName),
        "Lakehouse REST catalog provider %s discovered a blank catalog name",
        format);
    if (userConf.contains(SPARK_CATALOG_PREFIX + discoveredCatalogName)) {
      LOG.info(
          "Skip auto-registering {} catalog {} because it is configured by the user.",
          format,
          discoveredCatalogName);
      return;
    }
    if (!policy.shouldRegister(format, discoveredCatalogName)) {
      LOG.info(
          "Skip auto-registering {} catalog {} because catalog registration policy {} rejected it.",
          format,
          discoveredCatalogName,
          policy.getClass().getName());
      return;
    }

    String registeredCatalogName = policy.registeredCatalogName(format, discoveredCatalogName);
    // A blank name can only come from a policy: a blank discovered name already failed above.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(registeredCatalogName),
        "Catalog registration policy returned a blank catalog name");
    if (!isValidCatalogName(registeredCatalogName)) {
      // A user-configured policy chose this name, so failing fast points at code the user owns. The
      // default policy passes through what the REST server advertised, where one name Spark cannot
      // reference must not abort the whole Spark session.
      Preconditions.checkArgument(
          isDefaultPolicy(policy),
          "Catalog registration policy returned invalid Spark identifier: %s",
          registeredCatalogName);
      LOG.warn(
          "Skip auto-registering {} catalog {} because it is not a valid Spark identifier.",
          format,
          discoveredCatalogName);
      return;
    }
    Preconditions.checkArgument(
        !userConf.contains(SPARK_CATALOG_PREFIX + registeredCatalogName),
        "Catalog registration policy returned name %s, which is configured by the user",
        registeredCatalogName);
    Preconditions.checkArgument(
        registeredNames.add(registeredCatalogName),
        "Catalog registration policy returned duplicate name: %s",
        registeredCatalogName);

    Map<String, String> generatedProperties =
        provider.generatedCatalogProperties(uri, discoveredCatalogName);
    Preconditions.checkState(
        generatedProperties != null,
        "Lakehouse REST catalog provider %s returned null generated properties",
        format);
    Map<String, String> mergedProperties = new LinkedHashMap<>(globalProperties);
    generatedProperties.forEach(
        (key, value) -> {
          Preconditions.checkState(
              StringUtils.isNotBlank(key),
              "Lakehouse REST catalog provider %s returned a blank property name",
              format);
          Preconditions.checkState(
              value != null,
              "Lakehouse REST catalog provider %s returned null for property %s",
              format,
              key);
          mergedProperties.put(key, value);
        });

    registrations.add(
        new CatalogRegistration(
            format,
            discoveredCatalogName,
            registeredCatalogName,
            provider.catalogClassName(),
            mergedProperties));
  }

  /**
   * Returns whether Spark can reference {@code catalogName} unquoted. Gravitino accepts catalog
   * names that Spark's parser rejects, such as names containing a hyphen, so the answer is a value
   * rather than an exception: the caller decides whether that is a failure or a skip.
   */
  private static boolean isValidCatalogName(String catalogName) {
    if (StringUtils.isBlank(catalogName)) {
      return false;
    }
    // Called through ParserInterface, the type that declares `throws ParseException`, so the catch
    // can name that exception instead of swallowing every RuntimeException the parser may raise.
    ParserInterface parser = CatalystSqlParser$.MODULE$;
    try {
      Seq<String> parts = parser.parseMultipartIdentifier(catalogName);
      return parts.size() == 1 && catalogName.equals(parts.apply(0));
    } catch (ParseException e) {
      return false;
    }
  }

  private static boolean isDefaultPolicy(CatalogRegistrationPolicy policy) {
    return policy == DEFAULT_POLICY;
  }

  private static void applyRegistrations(
      SparkConf sparkConf, SparkConf userConf, List<CatalogRegistration> registrations) {
    for (CatalogRegistration registration : registrations) {
      String catalogPrefix = SPARK_CATALOG_PREFIX + registration.registeredCatalogName;
      sparkConf.set(catalogPrefix, registration.catalogClassName);
      registration.properties.forEach(
          (key, value) -> {
            String sparkConfigKey = catalogPrefix + "." + key;
            if (!userConf.contains(sparkConfigKey)) {
              sparkConf.set(sparkConfigKey, value);
            }
          });
      if (!registration.discoveredCatalogName.equals(registration.registeredCatalogName)) {
        LOG.info(
            "Register {} REST catalog {} as Spark catalog {}.",
            registration.format,
            registration.discoveredCatalogName,
            registration.registeredCatalogName);
      } else {
        LOG.info(
            "Register {} REST catalog {} in Spark.",
            registration.format,
            registration.discoveredCatalogName);
      }
    }
  }

  private static void registerSqlExtensions(SparkConf sparkConf, Set<String> extensions) {
    if (extensions.isEmpty()) {
      return;
    }

    String extensionsKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS().key();
    String[] providerExtensions = extensions.toArray(new String[0]);
    if (sparkConf.contains(extensionsKey) && StringUtils.isNotBlank(sparkConf.get(extensionsKey))) {
      sparkConf.set(
          extensionsKey,
          removeDuplicateSparkExtensions(
              providerExtensions, sparkConf.get(extensionsKey).split(COMMA)));
    } else {
      sparkConf.set(extensionsKey, String.join(COMMA, providerExtensions));
    }
  }

  private static ClassLoader contextClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null
        ? GravitinoLakehouseRESTDiscoveryDriverPlugin.class.getClassLoader()
        : classLoader;
  }

  private static class CatalogRegistration {
    private final String format;
    private final String discoveredCatalogName;
    private final String registeredCatalogName;
    private final String catalogClassName;
    private final Map<String, String> properties;

    private CatalogRegistration(
        String format,
        String discoveredCatalogName,
        String registeredCatalogName,
        String catalogClassName,
        Map<String, String> properties) {
      this.format = format;
      this.discoveredCatalogName = discoveredCatalogName;
      this.registeredCatalogName = registeredCatalogName;
      this.catalogClassName = catalogClassName;
      this.properties = properties;
    }
  }
}
