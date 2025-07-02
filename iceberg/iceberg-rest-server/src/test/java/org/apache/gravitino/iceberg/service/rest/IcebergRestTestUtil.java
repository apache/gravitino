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

package org.apache.gravitino.iceberg.service.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationExecutor;
import org.apache.gravitino.iceberg.service.extension.DummyCredentialProvider;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.apache.gravitino.iceberg.service.provider.StaticIcebergConfigProvider;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.logging.LoggingFeature.Verbosity;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.params.provider.Arguments;

public class IcebergRestTestUtil {

  private static final String V_1 = "v1";
  public static final String PREFIX = "prefix_gravitino";
  public static final String CONFIG_PATH = V_1 + "/config";
  public static final String NAMESPACE_PATH = V_1 + "/namespaces";
  public static final String UPDATE_NAMESPACE_POSTFIX = "properties";
  public static final Namespace TEST_NAMESPACE_NAME = Namespace.of("gravitino-test");
  public static final Namespace TEST_NESTED_NAMESPACE_NAME =
      Namespace.of("gravitino-test-2", "nested");

  public static final String VIEW_PATH =
      NAMESPACE_PATH + "/" + RESTUtil.encodeNamespace(TEST_NAMESPACE_NAME) + "/views";
  public static final String RENAME_TABLE_PATH = V_1 + "/tables/rename";

  public static final String RENAME_VIEW_PATH = V_1 + "/views/rename";
  public static final String REPORT_METRICS_POSTFIX = "metrics";

  public static final boolean DEBUG_SERVER_LOG_ENABLED = true;

  public static ResourceConfig getIcebergResourceConfig(Class c) {
    return getIcebergResourceConfig(c, true, Arrays.asList());
  }

  public static ResourceConfig getIcebergResourceConfig(
      Class c, boolean bindIcebergTableOps, List<EventListenerPlugin> eventListenerPlugins) {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(c);
    resourceConfig.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    resourceConfig.register(IcebergExceptionMapper.class);

    if (DEBUG_SERVER_LOG_ENABLED) {
      resourceConfig.register(
          new LoggingFeature(
              Logger.getLogger(LoggingFeature.DEFAULT_LOGGER_NAME),
              Level.INFO,
              Verbosity.PAYLOAD_ANY,
              10000));
    }

    if (bindIcebergTableOps) {
      Map<String, String> catalogConf = Maps.newHashMap();
      String catalogConfigPrefix = "catalog." + PREFIX;
      catalogConf.put(
          IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
          StaticIcebergConfigProvider.class.getName());
      catalogConf.put(String.format("%s.catalog-backend-name", catalogConfigPrefix), PREFIX);
      catalogConf.put(
          CredentialConstants.CREDENTIAL_PROVIDERS, DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE);
      catalogConf.put(
          String.format("%s.%s", catalogConfigPrefix, IcebergConstants.IO_IMPL),
          "org.apache.iceberg.aws.s3.S3FileIO");
      catalogConf.put(
          String.format("%s.%s", catalogConfigPrefix, IcebergConstants.ICEBERG_S3_ENDPOINT),
          "https://s3-endpoint.example.com");
      catalogConf.put(
          String.format("%s.%s", catalogConfigPrefix, IcebergConstants.AWS_S3_REGION), "us-west-2");
      catalogConf.put(
          String.format("%s.%s", catalogConfigPrefix, IcebergConstants.ICEBERG_OSS_ENDPOINT),
          "https://oss-endpoint.example.com");
      IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(catalogConf);
      configProvider.initialize(catalogConf);
      // used to override register table interface
      IcebergCatalogWrapperManager icebergCatalogWrapperManager =
          new IcebergCatalogWrapperManagerForTest(catalogConf, configProvider);

      EventBus eventBus = new EventBus(eventListenerPlugins);

      IcebergTableOperationExecutor icebergTableOperationExecutor =
          new IcebergTableOperationExecutor(icebergCatalogWrapperManager);
      IcebergTableEventDispatcher icebergTableEventDispatcher =
          new IcebergTableEventDispatcher(
              icebergTableOperationExecutor, eventBus, configProvider.getMetalakeName());
      IcebergViewOperationExecutor icebergViewOperationExecutor =
          new IcebergViewOperationExecutor(icebergCatalogWrapperManager);
      IcebergViewEventDispatcher icebergViewEventDispatcher =
          new IcebergViewEventDispatcher(
              icebergViewOperationExecutor, eventBus, configProvider.getMetalakeName());
      IcebergNamespaceOperationExecutor icebergNamespaceOperationExecutor =
          new IcebergNamespaceOperationExecutor(icebergCatalogWrapperManager);
      IcebergNamespaceEventDispatcher icebergNamespaceEventDispatcher =
          new IcebergNamespaceEventDispatcher(
              icebergNamespaceOperationExecutor, eventBus, configProvider.getMetalakeName());

      IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(new IcebergConfig());
      resourceConfig.register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bind(icebergCatalogWrapperManager).to(IcebergCatalogWrapperManager.class).ranked(2);
              bind(icebergMetricsManager).to(IcebergMetricsManager.class).ranked(2);
              bind(icebergTableEventDispatcher).to(IcebergTableOperationDispatcher.class).ranked(2);
              bind(icebergViewEventDispatcher).to(IcebergViewOperationDispatcher.class).ranked(2);
              bind(icebergNamespaceEventDispatcher)
                  .to(IcebergNamespaceOperationDispatcher.class)
                  .ranked(2);
            }
          });
    }
    return resourceConfig;
  }

  static HttpServletRequest createMockHttpRequest() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteHost()).thenReturn("localhost");
    when(mockRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
    return mockRequest;
  }

  public static Namespace[] testNamespaces() {
    return new Namespace[] {TEST_NAMESPACE_NAME, TEST_NESTED_NAMESPACE_NAME};
  }

  public static Stream<Arguments> testPrefixesAndNamespaces() {
    Namespace[] namespaces = testNamespaces();
    String[] prefixes = {"", PREFIX};
    return Arrays.stream(prefixes)
        .flatMap(
            prefix ->
                Arrays.stream(namespaces)
                    .flatMap(ns -> Stream.of(Arguments.arguments(prefix, ns))));
  }
}
