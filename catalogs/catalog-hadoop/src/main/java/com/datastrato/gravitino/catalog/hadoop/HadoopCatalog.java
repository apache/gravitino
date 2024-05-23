/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.catalog.hadoop.kerberos.KerberosConfig;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.ProxyPlugin;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.file.FilesetCatalog;
import java.util.Map;
import java.util.Optional;

/**
 * Hadoop catalog is a fileset catalog that can manage filesets on the Hadoop Compatible File
 * Systems, like Local, HDFS, S3, ADLS, etc, using the Hadoop FileSystem API. It can manage filesets
 * from different Hadoop Compatible File Systems in the same catalog.
 */
public class HadoopCatalog extends BaseCatalog<HadoopCatalog> {

  @Override
  public String shortName() {
    return "hadoop";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HadoopCatalogOperations ops = new HadoopCatalogOperations();
    return ops;
  }

  @Override
  protected Capability newCapability() {
    return new HadoopCatalogCapability();
  }

  @Override
  public FilesetCatalog asFilesetCatalog() {
    return (HadoopCatalogOperations) ops();
  }

  @Override
  protected Optional<ProxyPlugin> newProxyPlugin(Map<String, String> config) {
    boolean impersonationEnabled = new KerberosConfig(config).isImpersonationEnabled();
    if (!impersonationEnabled) {
      return Optional.empty();
    }
    return Optional.of(new HadoopProxyPlugin());
  }
}
