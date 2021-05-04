package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class __TestSecondaryAggregate {
  private static final Logger LOG = LoggerFactory.getLogger(__TestSecondaryAggregate.class);

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
  static Map<Long, List<App>> getExpectedResults(List<KV<Long, App>> input, int windowSize) {
    Map<Long, Map<String, Integer>> aggregates = new HashMap<>();
    for (KV<Long, App> elem : input) {
      long windowBegin = elem.getKey() - (elem.getKey() % (windowSize * 1000L));
      aggregates.putIfAbsent(windowBegin, new HashMap<>());
      aggregates.get(windowBegin).put(elem.getValue().getBundle(),
        aggregates.get(windowBegin).getOrDefault(elem.getValue().getBundle(), 0) + elem.getValue().getAmount());

    }
    Map<Long, List<App>> result = new HashMap<>();
    for (Entry<Long, Map<String, Integer>> et : aggregates.entrySet()) {
      List<App> sorted = et.getValue().entrySet().stream().sorted(new Comparator<Entry<String, Integer>>() {
        @Override public int compare(Entry<String, Integer> a, Entry<String, Integer> b) {
          return a.getValue() == b.getValue() ?
            a.getKey().compareTo(b.getKey()) :
            -a.getValue().compareTo(b.getValue());
        }
      }).map(kv -> App.newBuilder().setBundle(kv.getKey()).setAmount(kv.getValue()).build())
        .collect(Collectors.toList());
      List<App> output = new ArrayList<>();
      for (int i = 0; i < Math.min(3, sorted.size()); i++) {
        output.add(sorted.get(i));
      }
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


  // In all test cases, this PTransform will be called right before SecondaryAggregate step.
  static class PrimaryAggregate extends PTransform<PCollection<App>, PCollection<KV<String, Integer>>> {

    @Override public PCollection<KV<String, Integer>> expand(PCollection<App> data) {
      // We will use the default trigger with ON_TIME pane only.

      return data
        .apply(Window.<App>into(SlidingWindows.of(Duration.standardSeconds(30)).every(Duration.standardSeconds(10))))
        .apply(ParDo.of(new DoFn<App, KV<String, Integer>>() {
          @ProcessElement public void process(ProcessContext c) {
            c.output(KV.of(c.element().getBundle(), c.element().getAmount()));
          }
        })).apply(Sum.integersPerKey()).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
          @ProcessElement public void process(ProcessContext c) {
            // TODO - Useful for understanding input data & first stream job's output.
            //            LOG.info("[Primary] key: {} value: {}  pane: {}  window: {}", c.element().getKey(), c.element().getValue(),
            //              c.pane(), c.timestamp());

            c.output(c.element());
          }
        }));
    }
  }

  // ---------------

  @Test public void testSecondary00() {
    // In this case, the input is empty, so the output must be empty.
    // Note that the starter code already returns an empty PCollection, so you can leave it as-is to pass this test.

    PAssert.that(tp.apply(Create.
      empty(KvCoder.of(VarLongCoder.of(), ProtoCoder.of(App.class)))).apply(new AddTimestamp<App>())
      .apply(new PrimaryAggregate()).apply(new SecondaryAggregate(30))).empty();

    tp.run();
  }

  // -------------------------

  @Test public void __shareable__testSecondary00() {
    // In this case, the input is empty, so the output must be empty.
    // Note that the starter code already returns an empty PCollection, so you can leave it as-is to pass this test.

    PAssert.that(tp.apply(Create.
      empty(KvCoder.of(VarLongCoder.of(), ProtoCoder.of(App.class)))).apply(new AddTimestamp<App>())
      .apply(new PrimaryAggregate()).apply(new SecondaryAggregate(120))).empty();

    tp.run();
  }

  // -------------------------

  //
  //  @Test public void __hidden__testSecondaryStream00() {
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long gap = Duration.standardSeconds(15).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    // The following code creates an input dataset with the following elements:
  //
  //    //2021-05-01T10:15:15.000Z {"bundle":"app-0","amount":2}
  //    //2021-05-01T10:15:45.000Z {"bundle":"app-0","amount":3}
  //    //2021-05-01T10:16:15.000Z {"bundle":"app-0","amount":5}
  //
  //    //2021-05-01T10:16:00.000Z {"bundle":"app-1","amount":7}
  //    //2021-05-01T10:16:30.000Z {"bundle":"app-1","amount":11}
  //
  //    // If we want final output using 30-second fixed windows, we expect to see:
  //    // 2021-05-01T10:15:29.999Z: [ (app-0, 2) ]
  //    // 2021-05-01T10:15:59.999Z: [ (app-0, 3) ]
  //    // 2021-05-01T10:16:29.999Z: [ (app-1, 7), (app-0, 5) ]
  //    // 2021-05-01T10:16:59.999Z: [ (app-1, 11) ]
  //    // That is, we expect to see four 30-second fixed windows with List<App> as shown above.
  //
  //    // This unit test "simulates" elements' arrival and watermark/processing time advancement.
  //    // By the processing time (ts + 6 * gap), all five elements have arrived,
  //    // so the first stream job (PrimaryAggregate) will produce ON_TIMe panes accordingly for sliding windows.
  //    // SecondaryAggregate (your code) must produce four elements (one belonging to each window as illustrated above).
  //
  //    data.add(KV.of(ts + 0 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(2).build()));
  //    data.add(KV.of(ts + 2 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(3).build()));
  //    data.add(KV.of(ts + 4 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(5).build()));
  //
  //    data.add(KV.of(ts + 3 * gap, ab.setBundle(String.format("app-%d", 1)).setAmount(7).build()));
  //    data.add(KV.of(ts + 5 * gap, ab.setBundle(String.format("app-%d", 1)).setAmount(11).build()));
  //
  //    TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //    // The following code shuffles the input data in arbitrary order.
  //    // If your code's output is non-deterministic, this test may be flaky (and you'll need to fix it).
  //    streamBuilder = __TestTrickySum.getScrambledInputData(data, streamBuilder);
  //
  //    TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //    PCollection<List<App>> output = tp.apply(stream).apply(new PrimaryAggregate())//
  //      .apply(new SecondaryAggregate(30));
  //
  //    PAssert.that(output).satisfies(out -> {
  //      // You can observe what your output PC contains using the following code, or by using debugger.
  //      //      Iterator<List<App>> it = out.iterator();
  //      //      while (it.hasNext()) {
  //      //        System.out.format("%s\n", it.next().toString().replaceAll("\\n", " "));
  //      //      }
  //      assertEquals(4, Iterables.size(out));
  //      return null;
  //    });
  //
  //    // Window 1:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts - gap), Duration.standardSeconds(30)))
  //      .containsInAnyOrder(Arrays.asList(App.newBuilder().setBundle("app-0").setAmount(2).build()));
  //
  //    // Window 2:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts + gap), Duration.standardSeconds(30)))
  //      .containsInAnyOrder(Arrays.asList(App.newBuilder().setBundle("app-0").setAmount(3).build()));
  //
  //    // Window 3:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts + 3 * gap), Duration.standardSeconds(30)))
  //      .containsInAnyOrder(Arrays.asList( // Note of the order of Apps (must be sorted by amount in DESC first)
  //        App.newBuilder().setBundle("app-1").setAmount(7).build(),//
  //        App.newBuilder().setBundle("app-0").setAmount(5).build()));
  //
  //    // Window 4:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts + 5 * gap), Duration.standardSeconds(30)))
  //      .containsInAnyOrder(Arrays.asList(App.newBuilder().setBundle("app-1").setAmount(11).build()));
  //
  //    // Note: The above tests (PAssert statements) are equivalent to the following.
  //    // Tests with larger data only use this type of checking via 'getExpectedResults'.
  //    {
  //      final int windowSizeSecs = 30;
  //      Map<Long, List<App>> expected = getExpectedResults(data, windowSizeSecs);
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expected.size(), Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardSeconds(windowSizeSecs)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }
  //
  //
  //  @Test public void __hidden__testSecondaryStream01() {
  //    // Same test as 00, but with 90-second fixed windows.
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long gap = Duration.standardSeconds(15).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    // The following code creates an input dataset with the following elements:
  //
  //    //2021-05-01T10:15:15.000Z {"bundle":"app-0","amount":2}
  //    //2021-05-01T10:15:45.000Z {"bundle":"app-0","amount":3}
  //    //2021-05-01T10:16:15.000Z {"bundle":"app-0","amount":5}
  //
  //    //2021-05-01T10:16:00.000Z {"bundle":"app-1","amount":7}
  //    //2021-05-01T10:16:30.000Z {"bundle":"app-1","amount":11}
  //
  //    // If we want final output using 90-second fixed windows, we expect to see:
  //    // 2021-05-01T10:16:29.999Z: [ (app-0, 10), (app-1, 7) ]
  //    // 2021-05-01T10:17:59.999Z: [ (app-1, 11) ]
  //
  //    data.add(KV.of(ts + 0 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(2).build()));
  //    data.add(KV.of(ts + 2 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(3).build()));
  //    data.add(KV.of(ts + 4 * gap, ab.setBundle(String.format("app-%d", 0)).setAmount(5).build()));
  //
  //    data.add(KV.of(ts + 3 * gap, ab.setBundle(String.format("app-%d", 1)).setAmount(7).build()));
  //    data.add(KV.of(ts + 5 * gap, ab.setBundle(String.format("app-%d", 1)).setAmount(11).build()));
  //
  //    TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //    streamBuilder = __TestTrickySum.getScrambledInputData(data, streamBuilder);
  //
  //    TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //    PCollection<List<App>> output = tp.apply(stream).apply(new PrimaryAggregate())//
  //      .apply(new SecondaryAggregate(90));
  //
  //    PAssert.that(output).satisfies(out -> {
  //      assertEquals(2, Iterables.size(out));
  //      return null;
  //    });
  //
  //    // Window 1:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts - gap), Duration.standardSeconds(90)))
  //      .containsInAnyOrder(Arrays.asList(App.newBuilder().setBundle("app-0").setAmount(10).build(),
  //        App.newBuilder().setBundle("app-1").setAmount(7).build()));
  //
  //    // Window 2:
  //    PAssert.that(output).inWindow(new IntervalWindow(Instant.ofEpochMilli(ts + 5 * gap), Duration.standardSeconds(90)))
  //      .containsInAnyOrder(Arrays.asList(App.newBuilder().setBundle("app-1").setAmount(11).build()));
  //
  //    // Note: The above tests (PAssert statements) are equivalent to the following.
  //    // Tests with larger data only use this type of checking via 'getExpectedResults'.
  //    {
  //      final int windowSizeSecs = 90;
  //      Map<Long, List<App>> expected = getExpectedResults(data, windowSizeSecs);
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expected.size(), Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardSeconds(windowSizeSecs)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }
  //
  //  @Test public void __hidden__testSecondaryStream02() {
  //    // A bit more complex test with larger input.
  //
  //    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
  //    long gap = Duration.standardSeconds(11).getMillis();
  //    System.out.format("%d\n", ts);
  //    assertEquals(1619864115000L, ts);
  //
  //    App.Builder ab = App.newBuilder();
  //    List<KV<Long, App>> data = new ArrayList<>();
  //
  //    Random rnd = new Random(68686);
  //    for (int i = 0; i < 10; i++) {
  //      for (int j = 0; j < 5; j++) {
  //        long t = rnd.nextInt(10) + 1;
  //        int v = rnd.nextInt(20) + 1;
  //        data.add(KV.of(ts + t * gap, ab.setBundle(String.format("app-%d", i)).setAmount(v).build()));
  //      }
  //    }
  //
  //    for (int size = 30; size <= 150; size += 30) {
  //
  //      TestStream.Builder<App> streamBuilder = TestStream.create(ProtoCoder.of(App.class));
  //
  //      streamBuilder = __TestTrickySum.getScrambledInputData(data, streamBuilder);
  //
  //      TestStream<App> stream = streamBuilder.advanceWatermarkToInfinity();
  //
  //      PCollection<List<App>> output = tp.apply(stream).apply(new PrimaryAggregate())//
  //        .apply(new SecondaryAggregate(size));
  //
  //      Map<Long, List<App>> expected = getExpectedResults(data, size);
  //
  //
  //      PAssert.that(output).satisfies(out -> {
  //        assertEquals(expected.size(), Iterables.size(out));
  //        return null;
  //      });
  //
  //      for (Entry<Long, List<App>> et : expected.entrySet()) {
  //        PAssert.that(output)
  //          .inWindow(new IntervalWindow(Instant.ofEpochMilli(et.getKey()), Duration.standardSeconds(size)))
  //          .containsInAnyOrder(et.getValue());
  //      }
  //    }
  //
  //    tp.run().waitUntilFinish();
  //  }

}

