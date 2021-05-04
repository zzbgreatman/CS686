package edu.usfca.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface MyOptions extends DataflowPipelineOptions {
  @Description("Job name when running on GCP") @Default.String("cs686-proj6") String getJob();

  void setJob(String job);

  @Description("DirectRunner will be used if true") @Default.Boolean(true) boolean getIsLocal();

  void setIsLocal(boolean value);

  @Description("Useful for debugging.") @Default.String("?") String getJobSig();

  void setJobSig(String size);

  // ------------------

  @Description("See StreamJob.") @Default.Integer(0) int getWindowSize();

  void setWindowSize(int size);
}
