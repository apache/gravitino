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
package org.apache.gravitino.integration.test.container;

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
