package edu.usfca.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.SalesSummary;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToTableRow extends PTransform<PCollection<SalesSummary>, PCollection<TableRow>> {
  private static final Logger LOG = LoggerFactory.getLogger(ConvertToTableRow.class);

  final long JOB_SIG;

  public ConvertToTableRow(long jobSig) {
    this.JOB_SIG = jobSig;
  }

  /**
   * Output of ComputeSummary will be used as input to this PTransform.
   * <p>
   * The output of this step (TableRows) will be loaded to BigQuery (real-time).
   * <p>
   * Since we will produce only one TableRow per sliding window, we don't expect to see too many output elements from this step.
   */
  @Override public PCollection<TableRow> expand(PCollection<SalesSummary> input) {
    return input.apply(ParDo.of(new DoFn<SalesSummary, TableRow>() {
      @ProcessElement public void process(ProcessContext c) throws InvalidProtocolBufferException {
        TableRow row = new TableRow();
        // TODO -- add values to this object.


        c.output(row);

        // TODO - this will be useful for you keep track of what's in and what's out.
        LOG.info("[Converter] TableRow for ({}, {}) ready (from SS {})", JOB_SIG, c.element().getWindowEnd(),
          ProtoUtils.getJsonFromMessage(c.element(), true));
      }
    }));
  }
}
