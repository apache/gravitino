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
package org.apache.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import java.util.Optional;
import org.apache.gravitino.trino.connector.util.SpiVersionCompat;

/**
 * This class provides a ConnectorPageSink for Trino to write data to internal connector.
 *
 * <p>This shared provider backs every supported Trino version. Trino 435-479 use the non-credential
 * createPageSink/createMergeSink variants; Trino 480 added {@code
 * Optional<ConnectorTableCredentials>} variants and Trino 482 removed the non-credential ones and
 * made the credential variants abstract. To keep the shared source compiling against every
 * supported Trino SPI, the outbound calls are dispatched reflectively (each overload only exists on
 * part of the version range), and the credential variants are declared with raw {@code Optional}
 * parameters so {@code ConnectorTableCredentials} (absent before Trino 480) is never referenced at
 * compile time.
 */
@SuppressWarnings({"removal", "rawtypes", "unchecked"})
public class GravitinoPageSinkProvider implements ConnectorPageSinkProvider {

  ConnectorPageSinkProvider pageSinkProvider;

  /**
   * Constructs a new GravitinoPageSinkProvider with the specified page sink provider.
   *
   * @param pageSinkProvider the internal connector page sink provider
   */
  public GravitinoPageSinkProvider(ConnectorPageSinkProvider pageSinkProvider) {
    this.pageSinkProvider = pageSinkProvider;
  }

  // Not annotated @Override: this non-credential variant is the SPI method up to Trino 481 but was
  // removed in Trino 482. Kept so Trino 435-480 keep writing tables; dead on Trino 482.
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      ConnectorPageSinkId pageSinkId) {
    // GravitinoOutputTableHandle wraps a ConnectorInsertTableHandle internally,
    // so delegate to the insert-path createPageSink
    ConnectorInsertTableHandle insertHandle =
        ((GravitinoOutputTableHandle) outputTableHandle).getInternalHandle();
    return createInsertPageSink(transactionHandle, session, insertHandle, pageSinkId);
  }

  // Not annotated @Override: see the note on the ConnectorOutputTableHandle variant above.
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return createInsertPageSink(
        transactionHandle, session, GravitinoHandle.unWrap(insertTableHandle), pageSinkId);
  }

  // Not annotated @Override: see the note on the ConnectorOutputTableHandle variant above.
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorTableExecuteHandle tableExecuteHandle,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorPageSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createPageSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorTableExecuteHandle.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            GravitinoHandle.unWrap(tableExecuteHandle),
            pageSinkId);
  }

  // Not annotated @Override: see the note on the ConnectorOutputTableHandle variant above.
  public ConnectorMergeSink createMergeSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorMergeTableHandle mergeHandle,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorMergeSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createMergeSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorMergeTableHandle.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            GravitinoHandle.unWrap(mergeHandle),
            pageSinkId);
  }

  // Credential-aware variants: introduced in Trino 480 and abstract from Trino 482, so they must be
  // implemented for the shared source to compile against that SPI. They use raw Optional to avoid
  // naming ConnectorTableCredentials (absent before Trino 480) and delegate to the internal
  // connector's credential-aware overload, which is how Trino 481+ writes data (the non-credential
  // overloads above are only used on Trino 435-479). The reflective dispatch keeps this shared
  // source usable on every supported Trino version.
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      Optional tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    // GravitinoOutputTableHandle wraps a ConnectorInsertTableHandle internally, so delegate to the
    // insert-path createPageSink.
    ConnectorInsertTableHandle insertHandle =
        ((GravitinoOutputTableHandle) outputTableHandle).getInternalHandle();
    return createInsertPageSink(
        transactionHandle, session, insertHandle, tableCredentials, pageSinkId);
  }

  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      Optional tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return createInsertPageSink(
        transactionHandle,
        session,
        GravitinoHandle.unWrap(insertTableHandle),
        tableCredentials,
        pageSinkId);
  }

  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorTableExecuteHandle tableExecuteHandle,
      Optional tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorPageSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createPageSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorTableExecuteHandle.class,
              Optional.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            GravitinoHandle.unWrap(tableExecuteHandle),
            tableCredentials,
            pageSinkId);
  }

  public ConnectorMergeSink createMergeSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorMergeTableHandle mergeHandle,
      Optional tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorMergeSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createMergeSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorMergeTableHandle.class,
              Optional.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            GravitinoHandle.unWrap(mergeHandle),
            tableCredentials,
            pageSinkId);
  }

  private ConnectorPageSink createInsertPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorPageSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createPageSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorInsertTableHandle.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            insertTableHandle,
            pageSinkId);
  }

  private ConnectorPageSink createInsertPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      Optional tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return (ConnectorPageSink)
        SpiVersionCompat.invoke(
            pageSinkProvider,
            "createPageSink",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorInsertTableHandle.class,
              Optional.class,
              ConnectorPageSinkId.class
            },
            GravitinoHandle.unWrap(transactionHandle),
            session,
            insertTableHandle,
            tableCredentials,
            pageSinkId);
  }
}
