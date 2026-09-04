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

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/** Spark 3.5 write builder that exposes only governed batch append and optional truncate. */
@SuppressWarnings("deprecation")
final class GovernedDorisWriteBuilder35 implements WriteBuilder, SupportsTruncate {

  private final WriteBuilder delegate;
  private final DorisWritePolicy35 writePolicy;
  private final DorisWriteSchemaCompatibility35.Validator validator;

  GovernedDorisWriteBuilder35(
      WriteBuilder delegate,
      DorisWritePolicy35 writePolicy,
      DorisWriteSchemaCompatibility35.Validator validator) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  @Override
  public WriteBuilder truncate() {
    if (!writePolicy.allowsTruncate() || !(delegate instanceof SupportsTruncate)) {
      throw new UnsupportedOperationException(
          "The governed Doris connector does not support truncate overwrite");
    }
    ((SupportsTruncate) delegate).truncate();
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    if (!writePolicy.enabled()) {
      throw new UnsupportedOperationException(
          "The governed Doris connector does not support batch writes");
    }
    return new ValidatingBatchWrite(delegate.buildForBatch(), validator);
  }

  @Override
  public StreamingWrite buildForStreaming() {
    throw new UnsupportedOperationException(
        "The governed Doris connector does not support streaming writes");
  }

  private static final class ValidatingBatchWrite implements BatchWrite {

    private final BatchWrite delegate;
    private final DorisWriteSchemaCompatibility35.Validator validator;

    private ValidatingBatchWrite(
        BatchWrite delegate, DorisWriteSchemaCompatibility35.Validator validator) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.validator = validator;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new ValidatingDataWriterFactory(delegate.createBatchWriterFactory(info), validator);
    }

    @Override
    public boolean useCommitCoordinator() {
      return delegate.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
      delegate.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      delegate.commit(messages);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      delegate.abort(messages);
    }
  }

  private static final class ValidatingDataWriterFactory
      implements DataWriterFactory, Serializable {

    private static final long serialVersionUID = 1L;

    private final DataWriterFactory delegate;
    private final DorisWriteSchemaCompatibility35.Validator validator;

    private ValidatingDataWriterFactory(
        DataWriterFactory delegate, DorisWriteSchemaCompatibility35.Validator validator) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.validator = validator;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new ValidatingDataWriter(delegate.createWriter(partitionId, taskId), validator);
    }
  }

  private static final class ValidatingDataWriter implements DataWriter<InternalRow> {

    private final DataWriter<InternalRow> delegate;
    private final DorisWriteSchemaCompatibility35.Validator validator;

    private ValidatingDataWriter(
        DataWriter<InternalRow> delegate, DorisWriteSchemaCompatibility35.Validator validator) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.validator = validator;
    }

    @Override
    public void write(InternalRow record) throws IOException {
      validator.validate(record);
      delegate.write(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      return delegate.commit();
    }

    @Override
    public void abort() throws IOException {
      delegate.abort();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
      return delegate.currentMetricsValues();
    }
  }
}
