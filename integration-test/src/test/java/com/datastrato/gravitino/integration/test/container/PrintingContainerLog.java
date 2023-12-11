/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.output.OutputFrame.OutputType.END;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

// Printing Container Log
final class PrintingContainerLog extends BaseConsumer<PrintingContainerLog> {
  public static final Logger LOG = LoggerFactory.getLogger(PrintingContainerLog.class);
  private final String prefix;

  public PrintingContainerLog(String prefix) {
    this.prefix = requireNonNull(prefix, "prefix is null");
  }

  @Override
  public void accept(OutputFrame outputFrame) {
    // remove new line characters
    String message = outputFrame.getUtf8String().replaceAll("\\r?\\n?$", "");
    if (!message.isEmpty() || outputFrame.getType() != END) {
      LOG.info("{}{}", prefix, message);
    }
    if (outputFrame.getType() == END) {
      LOG.info("{}(exited)", prefix);
    }
  }
}
