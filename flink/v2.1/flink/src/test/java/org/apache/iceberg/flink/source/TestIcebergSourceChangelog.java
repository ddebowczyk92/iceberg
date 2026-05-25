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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergSourceChangelog {

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  public static MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, SimpleDataUtil.SCHEMA);

  @Test
  public void testInsertOnlyChangelog() throws Exception {
    GenericAppenderHelper appender = appender();

    appender.appendToTable(
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "aaa"), SimpleDataUtil.createRecord(2, "bbb")));
    appender.appendToTable(
        Lists.newArrayList(
            SimpleDataUtil.createRecord(3, "ccc"), SimpleDataUtil.createRecord(4, "ddd")));

    try (CloseableIterator<Row> iter = createChangelogStream().executeAndCollect("changelog")) {
      List<Row> results = TestIcebergSourceContinuous.waitForResult(iter, 4);

      assertThat(results).hasSize(4);
      assertThat(results).allMatch(r -> r.getKind() == RowKind.INSERT);
      assertThat(ids(results)).containsExactly(1, 2, 3, 4);
    }
  }

  @Test
  public void testInsertAndDeleteChangelog() throws Exception {
    GenericAppenderHelper appender = appender();
    DataFile dataFile1 =
        appender.writeFile(
            Lists.newArrayList(
                SimpleDataUtil.createRecord(1, "aaa"), SimpleDataUtil.createRecord(2, "bbb")));
    appender.appendToTable(dataFile1);
    appender.appendToTable(
        Lists.newArrayList(
            SimpleDataUtil.createRecord(3, "ccc"), SimpleDataUtil.createRecord(4, "ddd")));
    TABLE_EXTENSION.table().newDelete().deleteFile(dataFile1).commit();

    try (CloseableIterator<Row> iter = createChangelogStream().executeAndCollect("changelog")) {
      List<Row> results = TestIcebergSourceContinuous.waitForResult(iter, 6);
      assertInsertAndDeleteResults(results, 4, List.of(1, 2));
    }
  }

  @Test
  public void testContinuousChangelogStream(@InjectClusterClient ClusterClient<?> clusterClient)
      throws Exception {
    GenericAppenderHelper appender = appender();

    appender.appendToTable(
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "aaa"), SimpleDataUtil.createRecord(2, "bbb")));

    try (CloseableIterator<Row> iter = createChangelogStream().executeAndCollect("changelog")) {
      List<Row> result1 = TestIcebergSourceContinuous.waitForResult(iter, 2);
      assertThat(result1).hasSize(2);
      assertThat(result1).allMatch(r -> r.getKind() == RowKind.INSERT);

      TestIcebergSourceContinuous.waitUntilJobIsRunning(clusterClient);

      DataFile dataFile2 =
          appender.writeFile(
              Lists.newArrayList(
                  SimpleDataUtil.createRecord(3, "ccc"), SimpleDataUtil.createRecord(4, "ddd")));
      appender.appendToTable(dataFile2);

      List<Row> result2 = TestIcebergSourceContinuous.waitForResult(iter, 2);
      assertThat(result2).hasSize(2);
      assertThat(result2).allMatch(r -> r.getKind() == RowKind.INSERT);
      assertThat(ids(result2)).containsExactly(3, 4);

      TABLE_EXTENSION.table().newDelete().deleteFile(dataFile2).commit();

      List<Row> result3 = TestIcebergSourceContinuous.waitForResult(iter, 2);
      assertThat(result3).hasSize(2);
      assertThat(result3).allMatch(r -> r.getKind() == RowKind.DELETE);
      assertThat(ids(result3)).containsExactly(3, 4);
    }
  }

  @Test
  public void testSetAllOptionsPath() throws Exception {
    GenericAppenderHelper appender = appender();
    DataFile dataFile1 =
        appender.writeFile(
            Lists.newArrayList(
                SimpleDataUtil.createRecord(1, "aaa"), SimpleDataUtil.createRecord(2, "bbb")));
    appender.appendToTable(dataFile1);
    appender.appendToTable(
        Lists.newArrayList(
            SimpleDataUtil.createRecord(3, "ccc"), SimpleDataUtil.createRecord(4, "ddd")));
    TABLE_EXTENSION.table().newDelete().deleteFile(dataFile1).commit();

    try (CloseableIterator<Row> iter =
        createChangelogStream(ImmutableMap.of("scan.changelog-enabled", "true"))
            .executeAndCollect("changelog")) {
      List<Row> results = TestIcebergSourceContinuous.waitForResult(iter, 6);
      assertInsertAndDeleteResults(results, 4, List.of(1, 2));
    }
  }

  private GenericAppenderHelper appender() {
    Table table = TABLE_EXTENSION.table();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    return new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
  }

  private DataStream<Row> createChangelogStream() {
    return createChangelogStream(ImmutableMap.of());
  }

  private DataStream<Row> createChangelogStream(Map<String, String> extraOptions) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    IcebergSource.Builder<RowData> builder =
        IcebergSource.forRowData()
            .tableLoader(TABLE_EXTENSION.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .streaming(true)
            .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .monitorInterval(Duration.ofMillis(10L))
            .set("scan.changelog-enabled", "true");

    if (!extraOptions.isEmpty()) {
      builder.setAll(extraOptions);
    }

    return env.fromSource(
            builder.build(),
            WatermarkStrategy.noWatermarks(),
            "changelogSource",
            TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(SimpleDataUtil.SCHEMA)));
  }

  private static void assertInsertAndDeleteResults(
      List<Row> results, int expectedInserts, List<Integer> expectedDeletedIds) {
    assertThat(results).hasSize(expectedInserts + expectedDeletedIds.size());

    List<Row> inserts =
        results.stream().filter(r -> r.getKind() == RowKind.INSERT).collect(Collectors.toList());
    List<Row> deletes =
        results.stream().filter(r -> r.getKind() == RowKind.DELETE).collect(Collectors.toList());

    assertThat(inserts).hasSize(expectedInserts);
    assertThat(deletes).hasSize(expectedDeletedIds.size());
    assertThat(ids(deletes)).containsExactlyElementsOf(expectedDeletedIds);
  }

  private static List<Integer> ids(List<Row> rows) {
    return rows.stream().map(r -> (Integer) r.getField(0)).sorted().collect(Collectors.toList());
  }
}
