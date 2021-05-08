package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.GeneralUtils;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        return primary.apply(Window.into(FixedWindows.of(Duration.standardSeconds(windowSizeSecs))))
            .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, List<App>>>() {
                @ProcessElement public void process(ProcessContext c, BoundedWindow w) {
                    int numWindows = windowSizeSecs / 30;
                    boolean selected = false;
                    for (int i = 0; i < numWindows; i++) {
                        String keyTime = GeneralUtils.formatWindowEnd(w.maxTimestamp().getMillis() - i * 30 * 1000L);
                        if (GeneralUtils.formatWindowEnd(c.timestamp().getMillis()).equals(keyTime)) {
                            selected = true;
                        }
                    }
                    if (selected) {
                        List<App> resultList = new ArrayList<>();
                        resultList.add(
                            App.newBuilder().setBundle(c.element().getKey()).setAmount(c.element().getValue()).build());
                        c.output(KV.of(GeneralUtils.formatWindowEnd(w.maxTimestamp().getMillis()), resultList));
                    }
                }
            })).apply(Combine.perKey(new SerializableFunction<Iterable<List<App>>, List<App>>() {
                @Override public List<App> apply(Iterable<List<App>> input) {
                    Map<String, Integer> bundleAmount = new HashMap<>();
                    List<App> temp = new ArrayList<>();
                    List<App> result = new ArrayList<>();
                    for (List<App> l : input) {
                        for (App a : l) {
                            String bundle = a.getBundle();
                            if (!bundleAmount.containsKey(bundle)) {
                                bundleAmount.put(bundle, 0);
                            }
                            bundleAmount.put(bundle, bundleAmount.get(bundle) + a.getAmount());
                        }
                    }
                    for (String s : bundleAmount.keySet()) {
                        temp.add(App.newBuilder().setBundle(s).setAmount(bundleAmount.get(s)).build());
                    }
                    temp.sort(new GeneralUtils.AppComparator());

                    if (temp.size() <= 3) {
                        result.addAll(temp);
                    } else {
                        for (int i = 0; i < 3; i++) {
                            result.add(temp.get(i));
                        }
                    }
                    return result;
                }
            })).apply(Values.create());
    }
}
