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

import java.io.IOException;
import java.util.Locale;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

@Internal
public class IcebergSourceSplitSerializer implements SimpleVersionedSerializer<IcebergSplit> {
  private static final int VERSION = 4;

  static final byte TYPE_SOURCE_SPLIT = 0;
  static final byte TYPE_CHANGELOG_SPLIT = 1;

  private final boolean caseSensitive;

  public IcebergSourceSplitSerializer(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergSplit split) throws IOException {
    if (split instanceof IcebergSourceSplit) {
      return ((IcebergSourceSplit) split).serializeV4();
    } else if (split instanceof IcebergChangelogSourceSplit) {
      return ((IcebergChangelogSourceSplit) split).serializeV4();
    } else {
      throw new IOException("Unknown split type: " + split.getClass().getName());
    }
  }

  @Override
  public IcebergSplit deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return IcebergSourceSplit.deserializeV1(serialized);
      case 2:
        return IcebergSourceSplit.deserializeV2(serialized, caseSensitive);
      case 3:
        return IcebergSourceSplit.deserializeV3(serialized, caseSensitive);
      case 4:
        return deserializeV4(serialized);
      default:
        throw new IOException(
            String.format(
                Locale.ROOT,
                "Failed to deserialize IcebergSplit. "
                    + "Encountered unsupported version: %d. Supported versions are [1-4]",
                version));
    }
  }

  private IcebergSplit deserializeV4(byte[] serialized) throws IOException {
    byte type = serialized[0];
    byte[] inner = new byte[serialized.length - 1];
    System.arraycopy(serialized, 1, inner, 0, inner.length);
    switch (type) {
      case TYPE_SOURCE_SPLIT:
        return IcebergSourceSplit.deserializeV3(inner, caseSensitive);
      case TYPE_CHANGELOG_SPLIT:
        return IcebergChangelogSourceSplit.deserializeV1(inner);
      default:
        throw new IOException("Unknown split type tag: " + type);
    }
  }
}
