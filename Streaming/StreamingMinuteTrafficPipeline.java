/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mypackage.pipeline;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamingMinuteTrafficPipeline {

    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingMinuteTrafficPipeline.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Options extends DataflowPipelineOptions {
        @Description("Window duration length, in seconds")
        Integer getWindowDuration();
        void setWindowDuration(Integer windowDuration);

        @Description("BigQuery aggregate table name")
        String getAggregateTableName();
        void setAggregateTableName(String aggregateTableName);

        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("BigQuery raw table name")
        String getRawTableName();
        void setRawTableName(String rawTableName);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the {@link StreamingMinuteTrafficPipeline#run(Options)} method to
     * start the pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }


    /**
     * A DoFn acccepting Json and outputing CommonLog with Beam Schema
     */
    static class JsonToCommonLog extends DoFn<String, CommonLog> {

        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<CommonLog> r) throws Exception {
            Gson gson = new Gson();
            CommonLog commonLog = gson.fromJson(json, CommonLog.class);
            r.output(commonLog);
        }
    }

    /**
     * A Beam schema for counting pageviews per minute
     */
    public static final Schema pageViewsSchema = Schema
            .builder()
            .addInt64Field("pageviews")
            .addDateTimeField("minute")
            .build();

    public static final Schema rawSchema = Schema
            .builder()
            .addStringField("user_id")
            .addDateTimeField("event_timestamp")
            .addDateTimeField("processing_timestamp")
            .build();

    /**
     * Runs the pipeline to completion with the specified options. This method does
     * not wait until the pipeline is finished before returning. Invoke
     * {@code result.waitUntilFinish()} on the result object to block until the
     * pipeline is finished running if blocking programmatic execution is required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("streaming-minute-traffic-pipeline-" + System.currentTimeMillis());

        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */

        PCollection<CommonLog> commonLogs = pipeline
                //TODO: Read from PubSub
                 .apply("ReadFromPubSubTopic", PubsubIO.readStrings().withTimestampAttribute("timestamp").fromTopic(options.getInputTopic()))
                 .apply("ParseToCommonLog", ParDo.of(new JsonToCommonLog()))


        //TODO: Write aggregation logic and BQ Write
                .apply("WindowindEvery60Seconds", Window.<CommonLog>into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration()))));

                commonLogs.apply("CountPerWindow", Combine.globally(Count.<CommonLog>combineFn()).withoutDefaults())
                .apply("ConvertToRow", ParDo.of(new DoFn<Long, Row>() {
                    @ProcessElement
                    public void processElement(@Element Long input, OutputReceiver<Row> outputReceiver, IntervalWindow window) {
                     Instant instant = Instant.ofEpochMilli(window.end().getMillis());
                     Row row = Row.withSchema(pageViewsSchema)
                               .addValues(input, instant)
                               .build();
                     outputReceiver.output(row);     
                    }
                }
                )).setRowSchema(pageViewsSchema)

                .apply("WriteToBigQuery", 
                    BigQueryIO.<Row>write().to(options.getAggregateTableName()).useBeamSchema().withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
                
                



        // Write raw to BQ
        // But we want to add a processing time indicator as well
        commonLogs
                .apply("SelectFields", Select.fieldNames("user_id", "timestamp"))
                .apply("AddProcessingTimeField", AddFields.<Row>create().field("processing_timestamp", Schema.FieldType.DATETIME))
                .apply("AddProcessingTime", MapElements.via(new SimpleFunction<Row, Row>() {
                                                                @Override
                                                                public Row apply(Row row) {
                                                                    return Row.withSchema(rawSchema)
                                                                            .addValues(
                                                                                    row.getString("user_id"),
                                                                                    new DateTime(row.getString("timestamp")),
                                                                                    DateTime.now())
                                                                            .build();
                                                                }
                                                            }
                )).setRowSchema(rawSchema)
                .apply("WriteRawToBQ",
                        BigQueryIO.<Row>write().to(options.getRawTableName()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));



        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
