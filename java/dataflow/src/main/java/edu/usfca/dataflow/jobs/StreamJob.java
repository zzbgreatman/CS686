package edu.usfca.dataflow.jobs;

import com.google.api.services.bigquery.model.TableRow;
import edu.usfca.dataflow.Main;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

public class StreamJob extends PTransform<PBegin, PCollection<TableRow>> {
  final int WINDOW_SIZE_MIN;
  final long JOB_SIGNATURE;
  final String LOCAL_FILE_PATH;

  public StreamJob(int windowSize, long jobSig) {
    // TODO -- Do not change this code.
    this.WINDOW_SIZE_MIN = windowSize;
    this.JOB_SIGNATURE = jobSig;
    this.LOCAL_FILE_PATH = "";
  }

  public StreamJob(int windowSize, long jobSig, String filePath) {
    // TODO -- Do not change this code.
    this.WINDOW_SIZE_MIN = windowSize;
    this.JOB_SIGNATURE = jobSig;
    this.LOCAL_FILE_PATH = filePath;
  }

  @Override public PCollection<TableRow> expand(PBegin input) {
    PCollection<String> data;
    if (StringUtils.isBlank(LOCAL_FILE_PATH)) {
      data = input.apply(PubsubIO.readStrings()
        .fromTopic(String.format("projects/%s/topics/%s", Main.GCP_PROJECT_ID, Main.MAIN_TOPIC_ID)));
    } else {
      data = input.apply(TextIO.read().from(LOCAL_FILE_PATH));
    }
    // --------------------------------------------------------

    // TODO -- your code should begin with `data.apply(...)`.
    // Note that PubsubIO simply returns PC<String> where each String is a published message.
    // You will want to use Parser, ComputeSummary, and ConvertToTableRow that you have implemented.
    // FYI, the reference solution is a 1-liner (just a return statement).

    return null;
  }
}
