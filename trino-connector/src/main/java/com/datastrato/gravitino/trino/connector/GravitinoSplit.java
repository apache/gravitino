/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;

/**
 * The GravitinoFTransactionHandle is used to make Gravitino metadata operations transactional and
 * wrap the inner connector transaction for data access.
 */
public class GravitinoSplit implements ConnectorSplit, GravitinoHandle<ConnectorSplit> {

  private HandleWrapper<ConnectorSplit> handleWrapper = new HandleWrapper<>(ConnectorSplit.class);

  @JsonCreator
  public GravitinoSplit(@JsonProperty(HANDLE_STRING) String handleString) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  public GravitinoSplit(ConnectorSplit split) {
    this.handleWrapper = new HandleWrapper<>(split);
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorSplit getInternalHandle() {
    return handleWrapper.getHandle();
  }

  @Override
  public boolean isRemotelyAccessible() {
    return handleWrapper.getHandle().isRemotelyAccessible();
  }

  @Override
  public List<HostAddress> getAddresses() {
    return handleWrapper.getHandle().getAddresses();
  }

  @Override
  public Object getInfo() {
    return handleWrapper.getHandle().getInfo();
  }

  @Override
  public SplitWeight getSplitWeight() {
    return handleWrapper.getHandle().getSplitWeight();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return handleWrapper.getHandle().getRetainedSizeInBytes();
  }
}
