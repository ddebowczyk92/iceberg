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

import java.io.Serializable;
import org.apache.flink.api.connector.source.SourceSplit;

/**
 * Common interface for all Iceberg source splits. Both {@link IcebergSourceSplit} (append-only
 * reads) and {@link IcebergChangelogSourceSplit} (changelog reads with INSERT/DELETE) implement
 * this interface, allowing the enumerator, assigner, and reader infrastructure to handle them
 * uniformly.
 */
public interface IcebergSplit extends SourceSplit, Serializable {

  /** Record offset within the current file, used for checkpoint recovery. */
  long recordOffset();

  /** Estimated number of rows in this split, used for pending records metrics. */
  long estimatedRowsCount();

  /**
   * Update the checkpoint position within this split. For {@link IcebergSourceSplit}, both
   * fileOffset and recordOffset are tracked. For {@link IcebergChangelogSourceSplit}, only
   * recordOffset is meaningful (fileOffset is ignored since each split maps to a single file).
   */
  void updatePosition(int fileOffset, long newRecordOffset);
}
