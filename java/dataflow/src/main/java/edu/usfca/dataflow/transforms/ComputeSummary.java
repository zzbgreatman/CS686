package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesEvent;
import edu.usfca.protobuf.Common.SalesSummary;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeSummary extends PTransform<PCollection<SalesEvent>, PCollection<SalesSummary>> {
  final int WINDOW_SIZE_MIN; // Size of sliding time windows in minutes.
  private static final Logger LOG = LoggerFactory.getLogger(ComputeSummary.class);

  public ComputeSummary(int windowSize) {
    WINDOW_SIZE_MIN = windowSize;
  }

  /**
   * Using the output of "Parser" as input to this PTransform,
   * <p>
   * you will aggregate SalesEvent to produce SalesSummary.
   * <p>
   * One complication is that ONE SalesSummary should be produced per sliding window.
   * <p>
   * Since sliding windows have the same size, we only need to know each window's closing time (window_end),
   * <p>
   * which we'll use as the unique identifier for each window.
   * <p>
   * Again, this step must produce exactly ONE SalesSummary per (non-empty) sliding window.
   */
  @Override public PCollection<SalesSummary> expand(PCollection<SalesEvent> data) {
    // Note - you should apply Sliding Windowing function that begins every 10 seconds.
    // In __TestComputeSummary, it checks the number of output elements of this PTransform.
    // Then, it spot-checks those windows that end right on the minute.
    // You should check whether other sliding windows also contain correct results.


    return null;
  }
}
