/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.metrics.MetricsReport;

public class IcebergMetricsFormatter {

  private ObjectMapper icebergObjectMapper;

  IcebergMetricsFormatter() {
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
  }

  public String toPrintableString(MetricsReport metricsReport) {
    try {
      return toJson(metricsReport);
    } catch (JsonProcessingException e) {
      return metricsReport.toString();
    }
  }

  public String toJson(MetricsReport metricsReport) throws JsonProcessingException {
    return icebergObjectMapper.writeValueAsString(metricsReport);
  }
}
