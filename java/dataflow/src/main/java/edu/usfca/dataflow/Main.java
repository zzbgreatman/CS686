package edu.usfca.dataflow;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import edu.usfca.dataflow.jobs.StreamJob;
import edu.usfca.dataflow.transforms.ConvertToTableRow;
import edu.usfca.protobuf.Common.SalesSummary;
import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  // TODO: Make sure you change USER_EMAIL below to your @dons email address.
  final private static String USER_EMAIL = "zliu128@dons.usfca.edu";

  // TODO: Make sure you change the following four to your own settings.
  public final static String GCP_PROJECT_ID = "cs686-sp21-x33"; // <- TODO
  public final static String GCS_BUCKET = "gs://usf-my-bucket-x33"; // <- TODO

  public final static String MY_BQ_DATASET = "project6"; // <- TODO - Create this dataset in your project.

  public final static String MY_BQ_TABLE_OUT = "summary"; // <- Don't change.

  public final static TableReference BQ_DEST = // Don't change this.
    new TableReference().setProjectId(GCP_PROJECT_ID).setDatasetId(MY_BQ_DATASET).setTableId(MY_BQ_TABLE_OUT);

  public final static String MAIN_TOPIC_ID = "cs686-proj6-main"; // Don't change this.

  public final static String REGION = "us-west1"; // <- do not change.


  static String getUserEmail() {
    return USER_EMAIL;
  }

  public static void main(String[] args) {
    // TODO Take a look at MyOptions class in order to understand what command-line flags are available.
    MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);

    // TODO: Replace ?s with right types/mode.
    final TableSchema SCHEMA = new TableSchema().setFields(Arrays.asList(//
      new TableFieldSchema().setName("job_sig").setType("INT64").setMode("REQUIRED"), //
      new TableFieldSchema().setName("window_end").setType("STRING").setMode("REQUIRED"), //
      new TableFieldSchema().setName("top_apps").setType("STRUCT").setMode("REPEATED").setFields(Arrays.asList(//
        new TableFieldSchema().setName("bundle").setType("STRING"),//
        new TableFieldSchema().setName("amount").setType("INT64"),//
        new TableFieldSchema().setName("cnt_users").setType("INT64")//
      ))));

    switch (options.getJob()) {
      case "LETSGO": {

        options = setDefaultValues(options);

        final long jobSignature = Instant.now().toEpochMilli();
        options.setJobSig("" + jobSignature); // <- job signature can be found on DF Web Console.

        Pipeline p = Pipeline.create(options);

        p.apply(new StreamJob(options.getWindowSize(), jobSignature)).apply(
          BigQueryIO.writeTableRows().to(Main.BQ_DEST).withSchema(SCHEMA)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        LOG.info("NOTE: job_signature is: {}", jobSignature);

        p.run().waitUntilFinish();

        LOG.info("NOTE: job_signature is: {}", jobSignature);
        break;
      }

      case "input-file1": {
        options = setDefaultValues(options);

        final long jobSignature = Instant.now().toEpochMilli();

        Pipeline p = Pipeline.create(options);
        // TODO: If the local path below causes an issue, feel free to modify it.
        p.apply(new StreamJob(options.getWindowSize(), jobSignature, "resources/sample1.txt")).apply(
          BigQueryIO.writeTableRows().to(Main.BQ_DEST).withSchema(SCHEMA)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        p.run().waitUntilFinish();

        LOG.info("NOTE: job_signature is: {}", jobSignature);
        break;
      }

      case "input-file2": {
        options = setDefaultValues(options);

        final long jobSignature = Instant.now().toEpochMilli();

        Pipeline p = Pipeline.create(options);
        // TODO: If the local path below causes an issue, feel free to modify it.
        p.apply(new StreamJob(options.getWindowSize(), jobSignature, "resources/sample2.txt")).apply(
          BigQueryIO.writeTableRows().to(Main.BQ_DEST).withSchema(SCHEMA)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        p.run().waitUntilFinish();

        LOG.info("NOTE: job_signature is: {}", jobSignature);
        break;
      }

      case "CheckBq": {
        options = setDefaultValues(options);

        final long jobSignature = 686486L;

        Pipeline p = Pipeline.create(options);
        p.apply(Create.of(SalesSummary.newBuilder().setWindowEnd("00:00:59")//
          .addTopApps(App.newBuilder().setBundle("last project!").setCntUsers(486).setAmount(686).build())//
          .addTopApps(App.newBuilder().setBundle("how awesome!").setCntUsers(686).setAmount(486).build())//
          .addTopApps(App.newBuilder().setBundle("yay!").setCntUsers(486).setAmount(686).build())//
          .build())).apply(new ConvertToTableRow(jobSignature)).apply(
          BigQueryIO.writeTableRows().to(Main.BQ_DEST).withSchema(SCHEMA)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        p.run().waitUntilFinish();
        break;
      }

      default:
        break;
    }
  }


  /**
   * This sets default values for the Options class so that when you run your job on GCP, it won't complain about
   * missing parameters.
   * <p>
   * You don't have to change anything here.
   */
  static MyOptions setDefaultValues(MyOptions options) {
    System.out.format("user.dir: %s", System.getProperty("user.dir"));

    options.setJobName("proj6-consumer");

    options.setTempLocation(GCS_BUCKET + "/staging");
    if (options.getIsLocal()) {
      options.setRunner(DirectRunner.class);
    } else {
      options.setRunner(DataflowRunner.class);
    }
    if (options.getMaxNumWorkers() == 0) {
      options.setMaxNumWorkers(1);
    }
    if (StringUtils.isBlank(options.getWorkerMachineType())) {
      options.setWorkerMachineType("n1-standard-2");
    }
    options.setDiskSizeGb(150);
    options.setRegion(REGION);
    options.setProject(GCP_PROJECT_ID);

    // You will see more info here.
    // To run a pipeline (job) on GCP via Dataflow, you need to specify a few things like the ones above.
    LOG.info(options.toString());

    return options;
  }
}

