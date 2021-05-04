package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;

public class __TestTrickySum {
  private static final Logger LOG = LoggerFactory.getLogger(__TestTrickySum.class);

  // This is to help the grading system avoid 'hanging forever'
  // If unit tests keep failing due to 'timeout' on your local machine or when you need to use a debugger,
  // you should comment this out (this is only necessary for the grading system).
  @Rule public Timeout timeout = Timeout.seconds(10);

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Before public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }


  // This is used by unit tests to obtain a list of windows and their correct outputs
  // based on the input data.
  static Map<Long, List<App>> getExpectedResults(List<KV<Long, App>> input, int windowSize, int windowPeriod) {
    Map<Long, Map<String, Integer>> aggregates = new HashMap<>();
    for (KV<Long, App> elem : input) {
      long lhs = elem.getKey() - 60_000L * windowSize;
      lhs = lhs - (lhs % (windowSize * 60_000L));
      for (long wb = lhs; wb <= elem.getKey(); wb += 60_000L) {
        if (wb % (windowPeriod * 60_000L) != 0)
          continue;
        long we = wb + 60_000L * windowSize;
        if (wb <= elem.getKey() && elem.getKey() < we) {
          aggregates.putIfAbsent(wb, new HashMap<>());
          aggregates.get(wb).put(elem.getValue().getBundle(),
            aggregates.get(wb).getOrDefault(elem.getValue().getBundle(), 0) + elem.getValue().getAmount());
        }
      }
    }
    Map<Long, List<App>> result = new HashMap<>();
    for (Entry<Long, Map<String, Integer>> et : aggregates.entrySet()) {
      List<App> output = new ArrayList<>();
      output.addAll(et.getValue().entrySet().stream()
        .map(kv -> App.newBuilder().setBundle(kv.getKey()).setAmount(kv.getValue()).build())
        .collect(Collectors.toList()));
      result.put(et.getKey(), output);
    }
    return result;
  }

  // A useful PTransform that associates each element with timestamp.
  static class AddTimestamp<T> extends PTransform<PCollection<KV<Long, T>>, PCollection<T>> {

    @Override public PCollection<T> expand(PCollection<KV<Long, T>> input) {
      return input.apply(ParDo.of(new DoFn<KV<Long, T>, T>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), Instant.ofEpochMilli(c.element().getKey()));
        }
      }));
    }
  }


  // In all test cases, this PTransform will be called right before TrickySum step.
  static class PartialSumDiscarding extends PTransform<PCollection<App>, PCollection<KV<String, Integer>>> {

    final int WINDOW_SIZE;
    final int WINDOW_FREQUENCY;
    final int THRESHOLD;

    public PartialSumDiscarding(int size, int freq, int thresh) {
      if (size < 1 || freq < 1 || thresh < 1) {
        throw new IllegalArgumentException("bad parameters");
      }
      this.THRESHOLD = thresh;
      this.WINDOW_FREQUENCY = freq;
      this.WINDOW_SIZE = size;
    }

    @Override public PCollection<KV<String, Integer>> expand(PCollection<App> data) {
      // Note that this trigger is data-driven trigger that we studied in class.
      Trigger trigger =
        Repeatedly.forever(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(THRESHOLD)));

      return data.apply(Window.<App>into(
        SlidingWindows.of(Duration.standardMinutes(WINDOW_SIZE)).every(Duration.standardMinutes(WINDOW_FREQUENCY)))
        .withAllowedLateness(Duration.ZERO).triggering(trigger)//
        .discardingFiredPanes() // We are discarding fired panes. This is what makes this task tricky.
      ).apply(ParDo.of(new DoFn<App, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.output(KV.of(c.element().getBundle(), c.element().getAmount()));
        }
      })).apply(Sum.integersPerKey()).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          // This DoFn simply passes-through all elements, but prints out useful things.
          // You may find the following useful for debugging.
          // In the grading system, this is commented out to prevent excessive logs (which may time out unit tests).

          //          LOG
          //            .info("[Partial Sum] key: {} value: {}  pane: {}  window: {}", c.element().getKey(), c.element().getValue(),
          //              c.pane(), c.timestamp());

          c.output(c.element());
        }
      }));
    }
  }

  public static Builder<App> getScrambledInputData(List<KV<Long, App>> data, Builder<App> streamBuilder) {
    boolean[] added = new boolean[data.size()];
    Random rnd = new Random();
    long maxTs = Long.MIN_VALUE;
    for (int i = 0; i < data.size(); i++) {
      int x = rnd.nextInt(data.size());
      while (added[x])
        x = rnd.nextInt(data.size());
      added[x] = true;
      streamBuilder = streamBuilder
        .addElements(TimestampedValue.of(data.get(x).getValue(), Instant.ofEpochMilli(data.get(x).getKey())));
      maxTs = Math.max(maxTs, data.get(x).getKey());
    }
    streamBuilder = streamBuilder.advanceWatermarkTo(Instant.ofEpochMilli(maxTs + 1000L));
    return streamBuilder;
  }

  @Test public void testTricky00() {
    final int windowSize = 3;
    final int windowFrequency = 1;
    final int threshold = 100;

    // In this case, the input is empty, so the output must be empty.
    // Note that the starter code already returns an empty PCollection, so you can leave it as-is to pass this test.

    PCollection<App> output = tp.apply(Create.
      empty(KvCoder.of(VarLongCoder.of(), ProtoCoder.of(App.class)))).apply(new AddTimestamp<App>())
      .apply(new PartialSumDiscarding(windowSize, windowFrequency, threshold)).apply(new TrickySum());

    PAssert.that(output).empty();

    tp.run();
  }

  // -------------------------
  @Test public void __shareable__testTricky00() {
    final int windowSize = 5;
    final int windowFrequency = 2;
    final int threshold = 10;

    // In this case, the input is empty, so the output must be empty.
    // Note that the starter code already returns an empty PCollection, so you can leave it as-is to pass this test.

    PCollection<App> output = tp.apply(Create.
      empty(KvCoder.of(VarLongCoder.of(), ProtoCoder.of(App.class)))).apply(new AddTimestamp<App>())
      .apply(new PartialSumDiscarding(windowSize, windowFrequency, threshold)).apply(new TrickySum());

    PAssert.that(output).empty();

    tp.run();
  }

  // -------------------------

  //  @Test public void __hidden__Tricky00() {
  //    final int windowSize = 3;
  //    final int windowFrequency = 1;
  //    final int threshold = 1;
  //
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long min = Duration.standardMinutes(1).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    // We have the following six data points (for 1 app, for simplicity).
  //    //2021-05-01T10:15:15Z  (app-1, 1)
  //    //2021-05-01T10:17:15Z  (app-1, 2)
  //    //2021-05-01T10:17:20Z  (app-1, 4)
  //    //2021-05-01T10:19:15Z  (app-1, 8)
  //    //2021-05-01T10:21:15Z  (app-1, 16)
  //    //
  //    data.add(KV.of(ts + 0 * min, ab.setBundle("app-1").setAmount(1).build()));
  //    data.add(KV.of(ts + 2 * min, ab.setBundle("app-1").setAmount(2).build()));
  //    data.add(KV.of(ts + 2 * min + 5000L, ab.setBundle("app-1").setAmount(4).build()));
  //    data.add(KV.of(ts + 4 * min, ab.setBundle("app-1").setAmount(8).build()));
  //    data.add(KV.of(ts + 6 * min, ab.setBundle("app-1").setAmount(16).build()));
  //
  //    // With this data, the expected final output is:
  //    // window end:            value:
  //    // 2021-05-01T10:15:59Z  { (app-1, 1) }
  //    // 2021-05-01T10:16:59Z  { (app-1, 1) }
  //    // 2021-05-01T10:17:59Z  { (app-1, 7) }
  //    // 2021-05-01T10:18:59Z  { (app-1, 6) }
  //    // 2021-05-01T10:19:59Z  { (app-1, 14) }
  //    // 2021-05-01T10:20:59Z  { (app-1, 8) }
  //    // 2021-05-01T10:21:59Z  { (app-1, 24) }
  //    // 2021-05-01T10:22:59Z  { (app-1, 16) }
  //    // 2021-05-01T10:23:59Z  { (app-1, 16) }
  //    // That is, your PTransform should output a PC<App> with 9 elements, one for each window.
  //
  //    TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //    // The following code shuffles the input data in arbitrary order.
  //    // If your code's output is non-deterministic, this test may be flaky (and you'll need to fix it).
  //    streamBuilder = getScrambledInputData(data, streamBuilder);
  //
  //    TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //    PCollection<App> output =
  //      tp.apply(stream).apply(new PartialSumDiscarding(windowSize, windowFrequency, threshold)).apply(new TrickySum());
  //
  //    final org.joda.time.Instant firstTs = Instant.ofEpochMilli(ts - 15_000L - 2 * min);
  //
  //    // Window 1:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs, Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(1).build());
  //
  //    // Window 2:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(1).build());
  //
  //    // Window 3:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(2 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(7).build());
  //
  //    // Window 4:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(3 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(6).build());
  //
  //    // Window 5:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(4 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(14).build());
  //
  //    // Window 6:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(5 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(8).build());
  //
  //    // Window 7:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(6 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(24).build());
  //
  //    // Window 8:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(7 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(16).build());
  //
  //    // Window 9:
  //    PAssert.that(output).inWindow(new IntervalWindow(firstTs.plus(8 * min), Duration.standardMinutes(3)))
  //      .containsInAnyOrder(App.newBuilder().setBundle("app-1").setAmount(16).build());
  //
  //    PAssert.that(output).satisfies(out -> {
  //      assertEquals(9, Iterables.size(out));
  //      return null;
  //    });
  //
  //
  //    // Note: The above tests (PAssert statements) are equivalent to the following.
  //    // Tests with larger data only use this type of checking via 'getExpectedResults'.
  //    {
  //      Map<Long, List<App>> expected = getExpectedResults(data, windowSize, windowFrequency);
  //      final int expSize = expected.values().stream().map(list -> list.size()).reduce(0, Integer::sum);
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expSize, Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardMinutes(windowSize)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }
  //
  //  @Test public void __hidden__Tricky01() {
  //    final int windowSize = 3;
  //    final int windowFrequency = 2; // <-
  //    final int threshold = 1;
  //
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long min = Duration.standardMinutes(1).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    // Similar to test 00, but with more data points (and two apps).
  //    data.add(KV.of(ts + 0 * min, ab.setBundle("app-1").setAmount(1).build()));
  //    data.add(KV.of(ts + 2 * min, ab.setBundle("app-1").setAmount(2).build()));
  //    data.add(KV.of(ts + 2 * min + 5000L, ab.setBundle("app-1").setAmount(4).build()));
  //    data.add(KV.of(ts + 4 * min, ab.setBundle("app-1").setAmount(8).build()));
  //    data.add(KV.of(ts + 6 * min, ab.setBundle("app-1").setAmount(16).build()));
  //
  //    data.add(KV.of(ts + 0 * min, ab.setBundle("app-2").setAmount(1).build()));
  //    data.add(KV.of(ts + 2 * min, ab.setBundle("app-2").setAmount(2).build()));
  //    data.add(KV.of(ts + 2 * min + 5000L, ab.setBundle("app-2").setAmount(4).build()));
  //    data.add(KV.of(ts + 4 * min, ab.setBundle("app-2").setAmount(8).build()));
  //    data.add(KV.of(ts + 6 * min, ab.setBundle("app-2").setAmount(16).build()));
  //
  //    TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //    streamBuilder = getScrambledInputData(data, streamBuilder);
  //
  //    TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //    PCollection<App> output =
  //      tp.apply(stream).apply(new PartialSumDiscarding(windowSize, windowFrequency, threshold)).apply(new TrickySum());
  //
  //    {
  //      Map<Long, List<App>> expected = getExpectedResults(data, windowSize, windowFrequency);
  //      final int expSize = expected.values().stream().map(list -> list.size()).reduce(0, Integer::sum);
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expSize, Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardMinutes(windowSize)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }
  //
  //
  //  @Test public void __hidden__Tricky02() {
  //    final int windowSize = 5;
  //    final int windowFrequency = 2; // <-
  //    final int threshold = 2;
  //
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long min = Duration.standardMinutes(1).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    Random rnd = new Random(686);
  //    for (int i = 0; i < 5; i++) {
  //      for (int j = 0; j < 5; j++) {
  //        int t = rnd.nextInt(15) + 1, v = rnd.nextInt(15) + 1;
  //        data.add(KV.of(ts + t * min, ab.setBundle(String.format("app-%d", i)).setAmount(v).build()));
  //      }
  //    }
  //
  //    TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //    streamBuilder = getScrambledInputData(data, streamBuilder);
  //
  //    TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //    PCollection<App> output =
  //      tp.apply(stream).apply(new PartialSumDiscarding(windowSize, windowFrequency, threshold)).apply(new TrickySum());
  //
  //    {
  //      Map<Long, List<App>> expected = getExpectedResults(data, windowSize, windowFrequency);
  //      final int expSize = expected.values().stream().map(list -> list.size()).reduce(0, Integer::sum);
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expSize, Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardMinutes(windowSize)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }


}

