package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.SalesEvent;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This parser step should take into account that the input PC of Strings may or may not be bounded.
 * <p>
 * Each String element in the input PC is delimited (using MSG_DELIMITER below, hard-coded as " ").
 * We'll refer to each string (split by the delimiter) a "token".
 * <p>
 * If the source is bounded (e.g., data from TextIO or unit tests), then we assume that each input element (String) contains
 * timestamp (in unix millis) as the first token. The remaining tokens are Base64-encoded SalesEvent messages.
 * Recall from Lab 10 (and previous sample code) how you can associate each element with a specific timestamp.
 * <p>
 * If the source is unbounded (e.g., data from PubsubIO), each input element (String) must have been associated
 * with timestamp already, so each token in the String element is a Base64-encoded SalesEvent message.
 * <p>
 * With these assumptions, this PTransform must return PC of SalesEvent such that each element in the output PC
 * is associated with the correct timestamp. When the source is bounded, you should use the timestamp given.
 * Otherwise, you don't need to explicitly set timestamp for each element since the input element's timestamp is carried over automatically.
 * <p>
 * For this assignment, you may assume that every input String element is well-formatted and each token is a valid message.
 * <p>
 * You may find it useful to output logs using LOG.info() for debugging purposes.
 */
public class Parser extends PTransform<PCollection<String>, PCollection<SalesEvent>> {
  private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

  static final String MSG_DELIMITER = " "; // <- Use this.

  @Override public PCollection<SalesEvent> expand(PCollection<String> input) {
    PCollection<SalesEvent> data;

    if (input.isBounded() == IsBounded.BOUNDED) { // Input source is NOT PubSub.
      // TODO - in your DoFn, use the following code to produce logs.
      // This will be useful especially when you run a job on Google Dataflow.
      // You can also add pane info and timestamp, if you want.
      // LOG.info("[Parser] Processed the message: {}", c.element());

    } else { // Input source is PubSub. No need to explicitly declare timestamp.
      // TODO - in your DoFn, use the following code to produce logs.
      // This will be useful especially when you run a job on Google Dataflow.
      // You can also add pane info and timestamp, if you want.
      // LOG.info("[Parser] Processed the message: {}", c.element());
    }
    return null;
  }
}
