package edu.usfca.dataflow.transforms;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.utils.GeneralUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Common.SalesEvent;
import edu.usfca.protobuf.Common.SalesSummary;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
        return data.apply(Window
            .into(SlidingWindows.of(Duration.standardMinutes(WINDOW_SIZE_MIN)).every(Duration.standardSeconds(10L))))
            .apply(ParDo.of(new DoFn<SalesEvent, KV<String, SalesEvent>>() {
                @ProcessElement public void process(ProcessContext c, BoundedWindow w) {
                    c.output(KV.of(GeneralUtils.formatWindowEnd(w.maxTimestamp().getMillis()), c.element()));
                }
            })).apply(Combine.perKey(new Combine.CombineFn<SalesEvent, ArrayList<SalesEvent>, SalesSummary>() {

                @Override public ArrayList<SalesEvent> createAccumulator() {
                    return new ArrayList<>();
                }

                @Override
                public ArrayList<SalesEvent> addInput(ArrayList<SalesEvent> mutableAccumulator, SalesEvent input) {
                    mutableAccumulator.add(input);
                    return mutableAccumulator;
                }

                @Override public ArrayList<SalesEvent> mergeAccumulators(Iterable<ArrayList<SalesEvent>> accumulators) {
                    Iterator<ArrayList<SalesEvent>> it = accumulators.iterator();
                    ArrayList<SalesEvent> result = it.next();
                    while (it.hasNext()) {
                        result.addAll(it.next());
                    }
                    return result;
                }

                @Override public SalesSummary extractOutput(ArrayList<SalesEvent> accumulator) {
                    SalesSummary.Builder sb = SalesSummary.newBuilder();
                    HashMap<String, Integer> bundleAmountMap = new HashMap<>();
                    HashMap<String, Set<Common.DeviceId>> bundleCountMap = new HashMap<>();
                    for (SalesEvent s : accumulator) {
                        String bundle = s.getBundle();
                        if (!bundleAmountMap.containsKey(bundle)) {
                            bundleAmountMap.put(bundle, 0);
                        }
                        int amount = bundleAmountMap.get(bundle);
                        bundleAmountMap.put(bundle, amount + s.getAmount());
                        if (!bundleCountMap.containsKey(bundle)) {
                            bundleCountMap.put(bundle, new HashSet<>());
                        }
                        HashSet<Common.DeviceId> idSet = (HashSet<Common.DeviceId>) bundleCountMap.get(bundle);
                        idSet.add(Common.DeviceId.newBuilder().setOs(s.getId().getOs())
                            .setUuid(s.getId().getUuid().toUpperCase()).setWebid(s.getId().getWebid()).build());
                    }

                    List<SalesSummary.App> appArrayList = new ArrayList<>();

                    for (String b : bundleAmountMap.keySet()) {
                        appArrayList.add(SalesSummary.App.newBuilder().setBundle(b).setAmount(bundleAmountMap.get(b))
                            .setCntUsers(bundleCountMap.get(b).size()).build());
                    }

                    appArrayList.sort(new GeneralUtils.AppComparator());

                    if (appArrayList.size() <= 3) {
                        sb.addAllTopApps(appArrayList);
                    } else {
                        for (int i = 0; i < 3; i++) {
                            sb.addTopApps(appArrayList.get(i));
                        }
                    }
                    return sb.build();
                }
            })).apply(MapElements.via(new SimpleFunction<KV<String, SalesSummary>, SalesSummary>() {
                @Override public SalesSummary apply(KV<String, SalesSummary> elem) {
                    return elem.getValue().toBuilder().setWindowEnd(elem.getKey()).build();
                }
            }));

    }
}
