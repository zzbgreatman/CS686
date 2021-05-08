package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.utils.GeneralUtils;
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
            }))
            .apply(Combine.perKey(new Combine.CombineFn<SalesEvent, HashMap<String, List<SalesEvent>>, SalesSummary>() {

                @Override public HashMap<String, List<SalesEvent>> createAccumulator() {
                    return new HashMap<>();
                }

                @Override
                public HashMap<String, List<SalesEvent>> addInput(HashMap<String, List<SalesEvent>> mutableAccumulator,
                    SalesEvent input) {
                    String bundle = input.getBundle();
                    if (!mutableAccumulator.containsKey(bundle)) {
                        mutableAccumulator.put(bundle, new ArrayList<>());
                    }
                    List<SalesEvent> list = mutableAccumulator.get(bundle);
                    list.add(input);
                    return mutableAccumulator;
                }

                @Override public HashMap<String, List<SalesEvent>> mergeAccumulators(
                    Iterable<HashMap<String, List<SalesEvent>>> accumulators) {
                    Iterator<HashMap<String, List<SalesEvent>>> it = accumulators.iterator();
                    HashMap<String, List<SalesEvent>> result = new HashMap<>();

                    while (it.hasNext()) {
                        HashMap<String, List<SalesEvent>> now = it.next();
                        for (String s : now.keySet()) {
                            if (!result.containsKey(s)) {
                                result.put(s, new ArrayList<>());
                            }
                            ArrayList<SalesEvent> temp = (ArrayList<SalesEvent>) result.get(s);
                            temp.addAll(now.get(s));
                        }
                    }

                    HashMap<String, List<SalesEvent>> resultMap = new HashMap<>();

                    for (String s : result.keySet()) {
                        Map<Common.DeviceId, Integer> idAmount = new HashMap<>();
                        ArrayList<SalesEvent> tempList = (ArrayList<SalesEvent>) result.get(s);
                        ArrayList<SalesEvent> resultMapList = new ArrayList<>();

                        for (SalesEvent e : tempList) {
                            Common.DeviceId id = GeneralUtils.getNormalized(e).getId();
                            if (!idAmount.containsKey(id)) {
                                idAmount.put(id, 0);
                            }
                            int tempAmt = idAmount.get(id);
                            idAmount.put(id, tempAmt + e.getAmount());
                        }

                        for (Common.DeviceId di : idAmount.keySet()) {
                            resultMapList.add(
                                SalesEvent.newBuilder().setBundle(s).setAmount(idAmount.get(di)).setId(di).build());
                        }
                        resultMap.put(s, resultMapList);
                    }

                    return resultMap;
                }

                @Override public SalesSummary extractOutput(HashMap<String, List<SalesEvent>> accumulator) {
                    SalesSummary.Builder sb = SalesSummary.newBuilder();
                    for (String s : accumulator.keySet()) {
                        ArrayList<SalesEvent> tempList = (ArrayList<SalesEvent>) accumulator.get(s);
                        int ttlAmt = 0;
                        for (SalesEvent se : tempList) {
                            ttlAmt += se.getAmount();
                        }
                        sb.addTopApps(
                            SalesSummary.App.newBuilder().setBundle(s).setAmount(ttlAmt).setCntUsers(tempList.size())
                                .build());
                    }

                    return sb.build();
                }
            })).apply(ParDo.of(new DoFn<KV<String, SalesSummary>, SalesSummary>() {
                @ProcessElement public void process(ProcessContext c) {
                    SalesSummary.Builder sb = SalesSummary.newBuilder();
                    List<SalesSummary.App> appArrayList = new ArrayList<>();
                    appArrayList.addAll(c.element().getValue().getTopAppsList());
                    appArrayList.sort(new GeneralUtils.AppComparator());
                    if (appArrayList.size() <= 3) {
                        sb.addAllTopApps(appArrayList);
                    } else {
                        for (int i = 0; i < 3; i++) {
                            sb.addTopApps(appArrayList.get(i));
                        }
                    }
                    sb.setWindowEnd(c.element().getKey());
                    c.output(sb.build());
                }
            }));
    }
}
