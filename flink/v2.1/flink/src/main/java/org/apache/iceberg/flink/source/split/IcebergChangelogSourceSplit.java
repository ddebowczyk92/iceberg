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
package org.apache.iceberg.flink.source.split;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

/**
 * A source split for a single {@link ChangelogScanTask}. Maps one data file to a changelog
 * operation (INSERT or DELETE). The {@link TaskType} discriminator determines how the reader
 * processes deletes and sets {@link org.apache.flink.types.RowKind}.
 */
@Internal
public class IcebergChangelogSourceSplit implements IcebergSplit {
  private static final long serialVersionUID = 1L;

  private final ChangelogOperation operation;
  private final int changeOrdinal;
  private final long commitSnapshotId;
  private final DataFile dataFile;

  // Delete files to apply as filters during reading
  private final List<DeleteFile> deletes;

  // For DELETED_ROWS only: new delete files identifying which rows to emit as DELETE
  @Nullable private final List<DeleteFile> addedDeletes;

  private final TaskType taskType;
  private final PartitionSpec spec;
  private final long start;
  private final long length;
  private long recordOffset;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

  @Nullable private transient byte[] serializedBytesCache;

  private IcebergChangelogSourceSplit(
      ChangelogOperation operation,
      int changeOrdinal,
      long commitSnapshotId,
      DataFile dataFile,
      List<DeleteFile> deletes,
      @Nullable List<DeleteFile> addedDeletes,
      TaskType taskType,
      PartitionSpec spec,
      long start,
      long length,
      long recordOffset) {
    Preconditions.checkNotNull(operation, "operation cannot be null");
    Preconditions.checkNotNull(dataFile, "dataFile cannot be null");
    Preconditions.checkNotNull(deletes, "deletes cannot be null");
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    Preconditions.checkNotNull(spec, "spec cannot be null");
    if (taskType == TaskType.DELETED_ROWS) {
      Preconditions.checkNotNull(addedDeletes, "addedDeletes required for DELETED_ROWS");
    }

    this.operation = operation;
    this.changeOrdinal = changeOrdinal;
    this.commitSnapshotId = commitSnapshotId;
    this.dataFile = dataFile;
    this.deletes = deletes;
    this.addedDeletes = addedDeletes;
    this.taskType = taskType;
    this.spec = spec;
    this.start = start;
    this.length = length;
    this.recordOffset = recordOffset;
  }

  public static IcebergChangelogSourceSplit fromAddedRowsScanTask(AddedRowsScanTask task) {
    return new IcebergChangelogSourceSplit(
        task.operation(),
        task.changeOrdinal(),
        task.commitSnapshotId(),
        task.file(),
        task.deletes(),
        null,
        TaskType.ADDED_ROWS,
        task.spec(),
        task.start(),
        task.length(),
        0L);
  }

  public static IcebergChangelogSourceSplit fromDeletedDataFileScanTask(
      DeletedDataFileScanTask task) {
    return new IcebergChangelogSourceSplit(
        task.operation(),
        task.changeOrdinal(),
        task.commitSnapshotId(),
        task.file(),
        task.existingDeletes(),
        null,
        TaskType.DELETED_DATA_FILE,
        task.spec(),
        task.start(),
        task.length(),
        0L);
  }

  public static IcebergChangelogSourceSplit fromDeletedRowsScanTask(DeletedRowsScanTask task) {
    return new IcebergChangelogSourceSplit(
        task.operation(),
        task.changeOrdinal(),
        task.commitSnapshotId(),
        task.file(),
        task.existingDeletes(),
        task.addedDeletes(),
        TaskType.DELETED_ROWS,
        task.spec(),
        task.start(),
        task.length(),
        0L);
  }

  /** Dispatches to the appropriate factory method based on concrete task type. */
  public static IcebergChangelogSourceSplit fromChangelogScanTask(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return fromAddedRowsScanTask((AddedRowsScanTask) task);
    } else if (task instanceof DeletedDataFileScanTask) {
      return fromDeletedDataFileScanTask((DeletedDataFileScanTask) task);
    } else if (task instanceof DeletedRowsScanTask) {
      return fromDeletedRowsScanTask((DeletedRowsScanTask) task);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  public ChangelogOperation operation() {
    return operation;
  }

  public int changeOrdinal() {
    return changeOrdinal;
  }

  public long commitSnapshotId() {
    return commitSnapshotId;
  }

  public DataFile dataFile() {
    return dataFile;
  }

  public List<DeleteFile> deletes() {
    return deletes;
  }

  public List<DeleteFile> addedDeletes() {
    return addedDeletes != null ? addedDeletes : Collections.emptyList();
  }

  public TaskType taskType() {
    return taskType;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public long start() {
    return start;
  }

  public long length() {
    return length;
  }

  @Override
  public long recordOffset() {
    return recordOffset;
  }

  @Override
  public long estimatedRowsCount() {
    return dataFile.recordCount();
  }

  @Override
  public void updatePosition(int fileOffset, long newRecordOffset) {
    serializedBytesCache = null;
    recordOffset = newRecordOffset;
  }

  @Override
  public String splitId() {
    return MoreObjects.toStringHelper(this)
        .add("taskType", taskType)
        .add("operation", operation)
        .add("file", dataFile.location())
        .add("commitSnapshotId", commitSnapshotId)
        .add("changeOrdinal", changeOrdinal)
        .toString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("taskType", taskType)
        .add("operation", operation)
        .add("file", dataFile.location())
        .add("start", start)
        .add("length", length)
        .add("deletes", deletes.size())
        .add("addedDeletes", addedDeletes != null ? addedDeletes.size() : 0)
        .add("commitSnapshotId", commitSnapshotId)
        .add("changeOrdinal", changeOrdinal)
        .add("recordOffset", recordOffset)
        .toString();
  }

  /** Determines how the reader processes the data file and delete files. */
  public enum TaskType {
    /** Read data file, apply deletes, emit surviving rows as INSERT. */
    ADDED_ROWS,
    /** Read removed data file, apply existing deletes, emit surviving rows as DELETE. */
    DELETED_DATA_FILE,
    /** Read data file, emit only newly deleted rows as DELETE (filter out existing deletes). */
    DELETED_ROWS
  }

  // --- Serialization (V4 format: type tag + JSON payload via DataOutputSerializer) ---

  private static final String OPERATION = "operation";
  private static final String CHANGE_ORDINAL = "change-ordinal";
  private static final String COMMIT_SNAPSHOT_ID = "commit-snapshot-id";
  private static final String DATA_FILE = "data-file";
  private static final String DELETES = "deletes";
  private static final String ADDED_DELETES_FIELD = "added-deletes";
  private static final String TASK_TYPE = "task-type";
  private static final String SCHEMA = "schema";
  private static final String SPEC = "spec";
  private static final String START = "start";
  private static final String LENGTH = "length";
  private static final String RECORD_OFFSET = "record-offset";

  byte[] serializeV4() throws IOException {
    if (serializedBytesCache == null) {
      DataOutputSerializer out = SERIALIZER_CACHE.get();
      out.writeByte(IcebergSourceSplitSerializer.TYPE_CHANGELOG_SPLIT);

      String json =
          JsonUtil.generate(
              generator -> {
                generator.writeStartObject();

                generator.writeStringField(OPERATION, operation.name());
                generator.writeNumberField(CHANGE_ORDINAL, changeOrdinal);
                generator.writeNumberField(COMMIT_SNAPSHOT_ID, commitSnapshotId);

                generator.writeFieldName(DATA_FILE);
                ContentFileParser.toJson(dataFile, spec, generator);

                generator.writeArrayFieldStart(DELETES);
                for (DeleteFile del : deletes) {
                  ContentFileParser.toJson(del, spec, generator);
                }
                generator.writeEndArray();

                if (addedDeletes != null) {
                  generator.writeArrayFieldStart(ADDED_DELETES_FIELD);
                  for (DeleteFile del : addedDeletes) {
                    ContentFileParser.toJson(del, spec, generator);
                  }
                  generator.writeEndArray();
                }

                generator.writeStringField(TASK_TYPE, taskType.name());

                generator.writeFieldName(SCHEMA);
                SchemaParser.toJson(spec.schema(), generator);

                generator.writeFieldName(SPEC);
                PartitionSpecParser.toJson(spec, generator);

                generator.writeNumberField(START, start);
                generator.writeNumberField(LENGTH, length);
                generator.writeNumberField(RECORD_OFFSET, recordOffset);

                generator.writeEndObject();
              },
              false);

      SerializerHelper.writeLongUTF(out, json);
      serializedBytesCache = out.getCopyOfBuffer();
      out.clear();
    }
    return serializedBytesCache;
  }

  static IcebergChangelogSourceSplit deserializeV1(byte[] serialized) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    String json = SerializerHelper.readLongUTF(in);
    JsonNode node = JsonUtil.mapper().readTree(json);

    ChangelogOperation op = ChangelogOperation.valueOf(JsonUtil.getString(OPERATION, node));
    int ordinal = JsonUtil.getInt(CHANGE_ORDINAL, node);
    long snapshotId = JsonUtil.getLong(COMMIT_SNAPSHOT_ID, node);

    // Parse schema and spec — needed to parse content files
    Schema schema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, node));
    PartitionSpec parsedSpec = PartitionSpecParser.fromJson(schema, JsonUtil.get(SPEC, node));

    DataFile file = (DataFile) ContentFileParser.fromJson(node.get(DATA_FILE), parsedSpec);

    ImmutableList.Builder<DeleteFile> deletesBuilder = ImmutableList.builder();
    if (node.has(DELETES)) {
      for (JsonNode delNode : node.get(DELETES)) {
        deletesBuilder.add((DeleteFile) ContentFileParser.fromJson(delNode, parsedSpec));
      }
    }

    List<DeleteFile> addedDels = null;
    if (node.has(ADDED_DELETES_FIELD)) {
      ImmutableList.Builder<DeleteFile> addedBuilder = ImmutableList.builder();
      for (JsonNode delNode : node.get(ADDED_DELETES_FIELD)) {
        addedBuilder.add((DeleteFile) ContentFileParser.fromJson(delNode, parsedSpec));
      }
      addedDels = addedBuilder.build();
    }

    TaskType type = TaskType.valueOf(JsonUtil.getString(TASK_TYPE, node));
    long parsedStart = JsonUtil.getLong(START, node);
    long parsedLength = JsonUtil.getLong(LENGTH, node);
    long parsedRecordOffset = JsonUtil.getLong(RECORD_OFFSET, node);

    return new IcebergChangelogSourceSplit(
        op,
        ordinal,
        snapshotId,
        file,
        deletesBuilder.build(),
        addedDels,
        type,
        parsedSpec,
        parsedStart,
        parsedLength,
        parsedRecordOffset);
  }
}
