package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigIcebergTableOpsProvider implements IcebergTableOpsProvider {
  public static final Logger LOG = LoggerFactory.getLogger(ConfigIcebergTableOpsProvider.class);

  private IcebergConfig icebergConfig;

  @Override
  public void initialize(IcebergConfig config) {
    this.icebergConfig = config;
  }

  @Override
  public IcebergTableOps getIcebergTableOps(String prefix) {
    if (StringUtils.isBlank(prefix)) {
      return new IcebergTableOps(icebergConfig);
    }
    if (!getCatalogs().contains(prefix)) {
      String errorMsg = String.format("%s can not match any catalog", prefix);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return new IcebergTableOps(getCatalogConfig(prefix));
  }

  @Override
  public Optional<String> getPrefix(String warehouse) {
    if (StringUtils.isBlank(warehouse)) {
      return Optional.empty();
    }
    if (!getCatalogs().contains(warehouse)) {
      String errorMsg = String.format("%s can not match any catalog", warehouse);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return Optional.of(warehouse);
  }

  private List<String> getCatalogs() {
    Map<String, Boolean> catalogs = Maps.newHashMap();
    for (String key : this.icebergConfig.getAllConfig().keySet()) {
      if (!key.startsWith("catalog.")) {
        continue;
      }
      if (key.split("\\.").length < 3) {
        throw new RuntimeException(String.format("%s format is illegal", key));
      }
      catalogs.put(key.split("\\.")[1], true);
    }
    return catalogs.keySet().stream().sorted().collect(Collectors.toList());
  }

  private IcebergConfig getCatalogConfig(String catalog) {
    Map<String, String> base = Maps.newHashMap(this.icebergConfig.getAllConfig());
    Map<String, String> merge =
        MapUtils.getPrefixMap(
            this.icebergConfig.getAllConfig(), String.format("catalog.%s.", catalog));
    for (String key : merge.keySet()) {
      base.put(key, merge.get(key));
    }
    return new IcebergConfig(base);
  }
}
