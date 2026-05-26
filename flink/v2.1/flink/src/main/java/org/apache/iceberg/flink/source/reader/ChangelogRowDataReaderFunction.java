/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergChangelogSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reads {@link IcebergChangelogSourceSplit} and emits rows with appropriate {@link RowKind} based
 * on the changelog operation. INSERT operations emit {@link RowKind#INSERT}, DELETE operations emit
 * {@link RowKind#DELETE}.
 */
public class ChangelogRowDataReaderFunction implements ReaderFunction<RowData> {
  private final Schema tableSchema;
  private final Schema readSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final List<Expression> filters;
  private final int batchSize;
  private final RowType rowType;

  public ChangelogRowDataReaderFunction(
      ReadableConfig config,
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    this.tableSchema = tableSchema;
    this.readSchema = projectedSchema == null ? tableSchema : projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryption = encryption;
    this.filters = filters;
    this.batchSize = config.get(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT);
    this.rowType = FlinkSchemaUtil.convert(readSchema);
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> apply(
      IcebergSplit split) {
    IcebergChangelogSourceSplit changelogSplit = (IcebergChangelogSourceSplit) split;

    RowKind rowKind =
        changelogSplit.operation() == ChangelogOperation.INSERT ? RowKind.INSERT : RowKind.DELETE;

    // Build a synthetic FileScanTask from the changelog split
    List<DeleteFile> deletes;
    switch (changelogSplit.taskType()) {
      case ADDED_ROWS:
      case DELETED_DATA_FILE:
        deletes = changelogSplit.deletes();
        break;
      case DELETED_ROWS:
        // DELETED_ROWS requires differential delete logic: read only the rows newly
        // removed by added delete files. The core does not yet produce this task type
        // (BaseIncrementalChangelogScan rejects snapshots with delete manifests).
        throw new UnsupportedOperationException(
            "DELETED_ROWS task type is not yet supported in changelog scans");
      default:
        throw new UnsupportedOperationException("Unknown task type: " + changelogSplit.taskType());
    }

    String schemaString = SchemaParser.toJson(tableSchema);
    String specString = org.apache.iceberg.PartitionSpecParser.toJson(changelogSplit.spec());

    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(changelogSplit.spec(), Expressions.alwaysTrue(), caseSensitive);

    BaseFileScanTask fileScanTask =
        new BaseFileScanTask(
            changelogSplit.dataFile(),
            deletes.toArray(new DeleteFile[0]),
            schemaString,
            specString,
            residualEvaluator);

    RowDataFileScanTaskReader reader =
        new RowDataFileScanTaskReader(tableSchema, readSchema, nameMapping, caseSensitive, filters);

    InputFilesDecryptor decryptor =
        new InputFilesDecryptor(new BaseCombinedScanTask(fileScanTask), io, encryption);

    CloseableIterator<RowData> rowIterator = reader.open(fileScanTask, decryptor);

    return new ChangelogBatchIterator(
        changelogSplit.splitId(), rowIterator, rowKind, batchSize, rowType);
  }

  private static class ChangelogBatchIterator
      implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> {

    private final String splitId;
    private final CloseableIterator<RowData> rowIterator;
    private final RowKind rowKind;
    private final int batchSize;
    private final TypeSerializer[] fieldSerializers;
    private final RowData.FieldGetter[] fieldGetters;
    private final RowType rowType;
    private long recordOffset;

    ChangelogBatchIterator(
        String splitId,
        CloseableIterator<RowData> rowIterator,
        RowKind rowKind,
        int batchSize,
        RowType rowType) {
      this.splitId = splitId;
      this.rowIterator = rowIterator;
      this.rowKind = rowKind;
      this.batchSize = batchSize;
      this.rowType = rowType;
      this.fieldSerializers = RowDataRecordFactory.createFieldSerializers(rowType);
      this.fieldGetters = RowDataRecordFactory.createFieldGetters(rowType);
      this.recordOffset = 0;
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      List<RowData> batch = Lists.newArrayListWithCapacity(batchSize);
      long startOffset = recordOffset;

      while (rowIterator.hasNext() && batch.size() < batchSize) {
        RowData row = rowIterator.next();
        RowData cloned = RowDataUtil.clone(row, null, rowType, fieldSerializers, fieldGetters);
        cloned.setRowKind(rowKind);
        batch.add(cloned);
        recordOffset++;
      }

      return ListBatchRecords.forRecords(splitId, batch, 0, startOffset);
    }

    @Override
    public void close() throws IOException {
      rowIterator.close();
    }
  }
}
