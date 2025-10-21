/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryPojo;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JsonUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "compressiondictionary",
         description = "Manage compression dictionaries",
         subcommands = { CompressionDictionary.Train.class,
                         CompressionDictionary.List.class,
                         CompressionDictionary.Export.class,
                         CompressionDictionary.Import.class })
public class CompressionDictionary
{
    @Command(name = "train",
             description = "Manually trigger compression dictionary training for a table. If no SSTables are available, the memtable will be flushed first.")
    public static class Train extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Option(names = { "-f", "--force" }, description = "Force the dictionary training even if there are not enough samples")
        private boolean force = false;

        @Override
        public void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            PrintStream err = probe.output().err;

            try
            {
                out.printf("Starting compression dictionary training for %s.%s...%n", keyspace, table);
                out.printf("Training from existing SSTables (flushing first if needed)%n");

                probe.trainCompressionDictionary(keyspace, table, force);

                // Wait for training completion (10 minutes timeout for SSTable-based training)
                out.println("Sampling from existing SSTables and training.");
                long maxWaitMillis = TimeUnit.MINUTES.toMillis(10);
                long startTime = Clock.Global.currentTimeMillis();

                while (Clock.Global.currentTimeMillis() - startTime < maxWaitMillis)
                {
                    TrainingState trainingState = probe.getCompressionDictionaryTrainingState(keyspace, table);
                    TrainingStatus status = trainingState.getStatus();
                    displayProgress(trainingState, startTime, out, status);
                    if (TrainingStatus.COMPLETED == status)
                    {
                        out.printf("%nTraining completed successfully for %s.%s%n", keyspace, table);
                        return;
                    }
                    else if (TrainingStatus.FAILED == status)
                    {
                        err.printf("%nTraining failed for %s.%s%n", keyspace, table);
                        try
                        {
                            String failureMessage = trainingState.getFailureMessage();
                            if (failureMessage != null && !failureMessage.isEmpty())
                            {
                                err.printf("Reason: %s%n", failureMessage);
                            }
                        }
                        catch (Exception e)
                        {
                            // If we can't get the failure message, just continue without it
                        }
                        System.exit(1);
                    }

                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }

                err.printf("%nTraining did not complete within expected timeframe (10 minutes).%n");
                System.exit(1);
            }
            catch (Exception e)
            {
                err.printf("Failed to trigger training: %s%n", e.getMessage());
                System.exit(1);
            }
        }

        private static void displayProgress(TrainingState trainingState, long startTime, PrintStream out, TrainingStatus status)
        {
            // Display meaningful statistics
            long sampleCount = trainingState.getSampleCount();
            long totalSampleSize = trainingState.getTotalSampleSize();
            long elapsedSeconds = (Clock.Global.currentTimeMillis() - startTime) / 1000;
            double sampleSizeMB = totalSampleSize / (1024.0 * 1024.0);

            out.printf("\rStatus: %s | Samples: %d | Size: %.2f MiB | Elapsed: %ds",
                       status, sampleCount, sampleSizeMB, elapsedSeconds);
        }
    }

    @Command(name = "list", description = "List available dictionaries of specific keyspace and table.")
    public static class List extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Override
        protected void execute(NodeProbe probe)
        {
            try
            {
                TableBuilder tableBuilder = new TableBuilder();
                TabularData tabularData = probe.listCompressionDictionaries(keyspace, table);
                java.util.List<String> indexNames = tabularData.getTabularType().getIndexNames();

                java.util.List<String> columns = new ArrayList<>(indexNames);
                columns.remove(3);  // ignore raw dict
                tableBuilder.add(columns);

                for (Object eachDict : tabularData.keySet())
                {
                    final java.util.List<?> dictRow = (java.util.List<?>) eachDict;

                    java.util.List<String> rowValues = new ArrayList<>();

                    for (int i = 0; i < dictRow.size(); i++)
                    {
                        if (i == 3) // ignore raw dict
                            continue;

                        rowValues.add(dictRow.get(i).toString());
                    }
                    tableBuilder.add(rowValues);
                }

                tableBuilder.printTo(probe.output().out);
            }
            catch (Exception e)
            {
                probe.output().err.printf("Failed to list dictionaries: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }

    @Command(name = "export", description = "Export dictionary from Cassandra to local file.")
    public static class Export extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name", arity = "1")
        private String keyspace;

        @Parameters(index = "1", description = "The table name", arity = "1")
        private String table;

        @Parameters(index = "2", description = "The keyspace name", arity = "1")
        private long dictId;

        @Override
        protected void execute(NodeProbe probe)
        {
            try
            {
                CompositeData compressionDictionary = probe.getCompressionDictionary(keyspace, table, dictId);

                if (compressionDictionary == null)
                {
                    probe.output().err.printf("Given dictionary does not exist.%n");
                    System.exit(1);
                }

                CompressionDictionaryPojo pojo = CompressionDictionaryDetailsTabularData.from(compressionDictionary);
                probe.output().out.printf(JsonUtils.writeAsPrettyJsonString(pojo));
            }
            catch (Exception e)
            {
                probe.output().err.printf("Failed to export dictionary: %s%n", e.getMessage());
                System.exit(1);
            }
        }
    }

    @Command(name = "import", description = "Import local dictionary to Cassandra.")
    public static class Import extends AbstractCommand
    {
        @Parameters(index = "0", description = "The path to a dictionary JSON", arity = "1")
        private String dictionaryPath;

        @Override
        protected void execute(NodeProbe probe)
        {
            File dictionaryFile = new File(dictionaryPath);
            validateFile(dictionaryFile, probe);

            String jsonContent = FileUtils.readLines(dictionaryFile).stream().collect(Collectors.joining(System.lineSeparator()));

            try
            {
                CompressionDictionaryPojo dictionaryPojo = JsonUtils.JSON_OBJECT_MAPPER.readValue(jsonContent, CompressionDictionaryPojo.class);
                CompositeData compositeData = CompressionDictionaryDetailsTabularData.from(dictionaryPojo);
                probe.importCompressionDictionary(compositeData);
            }
            catch (Throwable t)
            {
                probe.output().err.printf("Unable to deserialize dictionary JSON: %s%n", t.getMessage());
                System.exit(1);
            }
        }

        private void validateFile(File dictionaryFile, NodeProbe probe)
        {
            if (!dictionaryFile.exists())
            {
                probe.output().err.printf("Path %s does not exist.%n", dictionaryPath);
                System.exit(1);
            }
            if (!dictionaryFile.isFile())
            {
                probe.output().err.printf("Path %s is not a file.%n", dictionaryPath);
                System.exit(1);
            }
            if (!dictionaryFile.isReadable())
            {
                probe.output().err.printf("Path %s is not readable.%n", dictionaryPath);
                System.exit(1);
            }
        }
    }
}
