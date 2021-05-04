package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrickySum extends PTransform<PCollection<KV<String, Integer>>, PCollection<App>> {
  private static final Logger LOG = LoggerFactory.getLogger(TrickySum.class);

  // "partial" is a partial sum (of "amount") per bundle, processed prior to this step.
  // More specifically, "PartialSumDiscarding" is the preceding PTransform.
  // Because the preceding step may produce multiple panes per window (each containing partial sum),
  // you must somehow find a way to aggregate them and produce a grand total per bundle, for each sliding window.
  // We do not use "cnt_users" field of App message in this task, for simplicity.
  // As a final output, this step should simply produce App for each unique (non-empty) sliding window and bundle.
  @Override public PCollection<App> expand(PCollection<KV<String, Integer>> partial) {
    SlidingWindows sw = (SlidingWindows) partial.getWindowingStrategy().getWindowFn();
    // ^ From this object, you can retrieve sliding window's size and period (frequency), which may be useful.

    // TODO - Finish implementing this.
    return partial.getPipeline().apply(Create.empty(TypeDescriptor.of(App.class)));
  }
}
