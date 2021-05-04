package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;

public class SecondaryAggregate extends PTransform<PCollection<KV<String, Integer>>, PCollection<List<App>>> {
  final int windowSizeSecs;

  public SecondaryAggregate(int windowSizeSecs) {
    if (windowSizeSecs % 30 != 0) {
      throw new IllegalArgumentException("windowSizeSecs must be a multiple of 30, but got: " + windowSizeSecs);
    }
    this.windowSizeSecs = windowSizeSecs;
  }

  // "primary" is a sum (of "amount") per bundle, processed prior to this step.
  // You should expect multiple KVs per sliding window with ON_TIME timing (but only one KV per bundle per window).
  // This step must produce top 3 apps per x-second fixed window, where x is always a multiple of 30.
  // NOTE: You can (and should) exploit this fact about x in your code.
  //
  // As a final output, this step should simply produce one List<App> for each (non-empty) fixed window
  // where List contains (up to) three best-selling apps (ordered by amount (in DESC) then by bundle (in ASC)).
  // You may re-use your AppComparator from GeneralUtils (reference solution uses it for convenience).
  @Override public PCollection<List<App>> expand(PCollection<KV<String, Integer>> primary) {
    // This returns an empty PC, so you pass the sample test.
    // TODO - Implement this.
    return primary.getPipeline().
      apply(Create.empty(TypeDescriptors.
        lists(TypeDescriptor.of(App.class))));
  }
}
