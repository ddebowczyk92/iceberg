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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.iceberg.flink.source.split.IcebergSplit;
import org.apache.iceberg.flink.source.split.SerializableComparator;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class IcebergSourceReader<T>
    extends SingleThreadMultiplexSourceReaderBase<
        RecordAndPosition<T>, T, IcebergSplit, IcebergSplit> {

  public IcebergSourceReader(
      SerializableRecordEmitter<T> emitter,
      IcebergSourceReaderMetrics metrics,
      ReaderFunction<T> readerFunction,
      SerializableComparator<IcebergSplit> splitComparator,
      SourceReaderContext context) {
    super(
        () -> new IcebergSourceSplitReader<>(metrics, readerFunction, splitComparator, context),
        emitter,
        context.getConfiguration(),
        context);
  }

  @Override
  public void start() {
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      requestSplit(Collections.emptyList());
    }
  }

  @Override
  protected void onSplitFinished(Map<String, IcebergSplit> finishedSplitIds) {
    requestSplit(Lists.newArrayList(finishedSplitIds.keySet()));
  }

  @Override
  protected IcebergSplit initializedState(IcebergSplit split) {
    return split;
  }

  @Override
  protected IcebergSplit toSplitType(String splitId, IcebergSplit splitState) {
    return splitState;
  }

  private void requestSplit(Collection<String> finishedSplitIds) {
    context.sendSourceEventToCoordinator(new SplitRequestEvent(finishedSplitIds));
  }
}
