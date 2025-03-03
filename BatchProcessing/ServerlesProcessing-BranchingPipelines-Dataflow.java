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

import javax.annotation.Nullable;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Instant;


public class MyPipeline {

    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Options extends DataflowPipelineOptions {
        @Description("Name of the user who is running the pipeline.")
        @Default.String("DEFAULT")
        String getUserRunningPipeline();
        void setUserRunningPipeline(String userRunningPipeline);

        @Description("Input path for the pipeline")
        @Default.String("DEFAULT")
        String getInputPath();
        void setInputPath(String inputPath);

        @Description("Google Storage bucket Path")
        @Default.String("DEFAULT")
        String getGoogleStoragePath();
        void setGoogleStoragePath(String googleStoragePath);

        @Description("Big Query table name")
        @Default.String("DEFAULT")
        String getBigQueryTableName();
        void setBigQueryTableName(String bigQueryTableName);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the {@link MyPipeline#run(Options)} method to
     * start the pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class); // Register Custom interface

        Options options = PipelineOptionsFactory.fromArgs(args) // Getting args from cli
        .withValidation() // Validate args
        .as(Options.class); // Save as Options instance into options

        run(options);
    }

    /**
     * A class used for parsing JSON web server events
     * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
     */
    @DefaultSchema(JavaFieldSchema.class)
    public static class CommonLog {
        String user_id;
        String ip;
        @Nullable Double lat;
        @Nullable Double lng;
        String timestamp;
        String http_request;
        String user_agent;
        Long http_response;
        Long num_bytes;
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
        options.setJobName("my-pipeline-" + System.currentTimeMillis());


        final String input = options.getInputPath();
        final String gcsPath = options.getGoogleStoragePath();
        final String bqTableName = options.getBigQueryTableName();

        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */
        

        PCollection<String> pipelineCollection = pipeline.apply("ReadFromGCS", TextIO.read().from(input));
                
        pipelineCollection.apply("WriteRawToGCS", TextIO.write().to(gcsPath));

        pipelineCollection.apply("ParseJson", ParDo.of(new JsonToCommonLog()))
                .apply("DropPII", DropFields.fields("user_agent"))            
                .apply("ConvertToRow", Select.fieldNames("ip", "lat", "lng", "timestamp", "num_bytes"))
                .apply("Filtering", Filter.<Row>create()
                    .whereFieldName("num_bytes", (Long nb) -> nb > 150 && nb <= 400)
                    .whereFieldName("timestamp", (String ts) -> {
                        Instant instant = Instant.parse(ts);
                        Instant threshold = Instant.parse("2025-03-03T22:07:40.214042Z");
                        return instant.isAfter(threshold);
                    } ))
                .apply("WriteToBQ",
                        BigQueryIO.<Row>write().to(bqTableName).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
