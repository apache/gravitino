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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

/** Tests governed delegation for Doris batch append and explicit truncate. */
@SuppressWarnings("deprecation")
public class TestGovernedDorisWriteBuilder35 {

  @Test
  void testDelegatesOnlyBatchAndExplicitTruncate() {
    WriteBuilder delegate =
        mock(WriteBuilder.class, withSettings().extraInterfaces(SupportsTruncate.class));
    BatchWrite batch = mock(BatchWrite.class);
    when(delegate.buildForBatch()).thenReturn(batch);
    when(batch.useCommitCoordinator()).thenReturn(true);
    when(((SupportsTruncate) delegate).truncate()).thenReturn(delegate);
    DorisWritePolicy35 policy = truncatePolicy();
    GovernedDorisWriteBuilder35 builder =
        new GovernedDorisWriteBuilder35(
            delegate, policy, DorisWriteSchemaCompatibility35.Validator.none());

    assertSame(builder, builder.truncate());
    BatchWrite governedBatch = builder.buildForBatch();
    assertNotSame(batch, governedBatch);
    assertTrue(governedBatch.useCommitCoordinator());
    WriterCommitMessage message = mock(WriterCommitMessage.class);
    WriterCommitMessage[] messages = new WriterCommitMessage[] {message};
    governedBatch.onDataWriterCommit(message);
    governedBatch.commit(messages);
    governedBatch.abort(messages);
    assertThrows(UnsupportedOperationException.class, builder::buildForStreaming);
    verify((SupportsTruncate) delegate).truncate();
    verify(delegate).buildForBatch();
    verify(batch).useCommitCoordinator();
    verify(batch).onDataWriterCommit(message);
    verify(batch).commit(messages);
    verify(batch).abort(messages);
  }

  @Test
  void testValidatesDatetimeBeforeDelegatingRows() throws Exception {
    WriteBuilder delegate = mock(WriteBuilder.class);
    BatchWrite batch = mock(BatchWrite.class);
    DataWriterFactory factory = mock(DataWriterFactory.class);
    @SuppressWarnings("unchecked")
    DataWriter<InternalRow> writer = mock(DataWriter.class);
    PhysicalWriteInfo info = mock(PhysicalWriteInfo.class);
    when(delegate.buildForBatch()).thenReturn(batch);
    when(batch.createBatchWriterFactory(info)).thenReturn(factory);
    when(factory.createWriter(0, 1L)).thenReturn(writer);
    WriterCommitMessage commitMessage = mock(WriterCommitMessage.class);
    when(writer.commit()).thenReturn(commitMessage);
    CustomTaskMetric[] metrics = new CustomTaskMetric[] {mock(CustomTaskMetric.class)};
    when(writer.currentMetricsValues()).thenReturn(metrics);
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of(
                  "event_time",
                  Types.TimestampType.withoutTimeZone(3),
                  null,
                  true,
                  false,
                  Column.DEFAULT_VALUE_NOT_SET)
            });
    StructType stringSchema = new StructType().add("event_time", DataTypes.StringType, true);
    DorisWriteSchemaCompatibility35.Validator validator =
        DorisWriteSchemaCompatibility35.validate(
            logicalTable,
            new DorisReadSchema35(
                stringSchema,
                ImmutableList.of("`event_time`"),
                true,
                ImmutableMap.of("event_time", "DATETIMEV2(3)")),
            stringSchema);
    GovernedDorisWriteBuilder35 builder =
        new GovernedDorisWriteBuilder35(delegate, batchPolicy(), validator);
    DataWriter<InternalRow> governedWriter =
        builder.buildForBatch().createBatchWriterFactory(info).createWriter(0, 1L);
    GenericInternalRow valid =
        new GenericInternalRow(new Object[] {UTF8String.fromString("2026-08-14 00:00:00.123")});
    governedWriter.write(valid);
    verify(writer).write(valid);
    assertSame(commitMessage, governedWriter.commit());
    governedWriter.abort();
    governedWriter.close();
    verify(writer).commit();
    verify(writer).abort();
    verify(writer).close();
    assertSame(metrics, governedWriter.currentMetricsValues());
    verify(writer).currentMetricsValues();

    GenericInternalRow invalid =
        new GenericInternalRow(new Object[] {UTF8String.fromString("secret-value")});
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> governedWriter.write(invalid));
    assertFalse(error.getMessage().contains("secret-value"));
    verify(writer, never()).write(invalid);
  }

  @Test
  void testDefaultOverwritePolicyRejectsBeforeDelegateMutation() {
    WriteBuilder delegate =
        mock(WriteBuilder.class, withSettings().extraInterfaces(SupportsTruncate.class));
    GovernedDorisWriteBuilder35 builder =
        new GovernedDorisWriteBuilder35(
            delegate, batchPolicy(), DorisWriteSchemaCompatibility35.Validator.none());

    assertThrows(UnsupportedOperationException.class, builder::truncate);
    verifyNoInteractions(delegate);
  }

  private static DorisWritePolicy35 batchPolicy() {
    return DorisWritePolicy35.from(
        ImmutableMap.of(
            DorisConnectorConstants35.GRAVITINO_WRITE_MODE, DorisConnectorConstants35.WRITE_BATCH));
  }

  private static DorisWritePolicy35 truncatePolicy() {
    return DorisWritePolicy35.from(
        ImmutableMap.of(
            DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
            DorisConnectorConstants35.WRITE_BATCH,
            DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
            DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE));
  }
}
