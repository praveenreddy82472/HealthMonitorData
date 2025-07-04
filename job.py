# Apache Beam Dataflow job to write batch and stream data to separate BigQuery tables

import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.textio import ReadFromText
from apache_beam.transforms.window import FixedWindows
import datetime

# ------------------------------
# Helper Functions
# ------------------------------

def parse_batch_csv(line):
    import csv
    from io import StringIO
    reader = csv.reader(StringIO(line))
    fields = next(reader)
    return {
        "Patient_ID": fields[0],
        "Age": int(fields[1]),
        "Gender": fields[2],
        "Condition": fields[3],
        "Procedure": fields[4],
        "Cost": float(fields[5]),
        "Length_of_Stay": int(fields[6]),
        "Readmission": fields[7],
        "Outcome": fields[8],
        "Satisfaction": fields[9]
    }

def parse_stream_json(message):
    record = json.loads(message.decode('utf-8'))
    record['event_timestamp'] = datetime.datetime.utcnow().isoformat()
    return {
        "Patient_ID": record["Patient_ID"],
        "Temperature_C": float(record["Temperature_C"]),
        "Blood_Pressure": record["Blood_Pressure"],
        "Heart_Rate": int(record["Heart_Rate"]),
        "event_timestamp": record["event_timestamp"]
    }

# ------------------------------
# Main pipeline function
# ------------------------------

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--input_batch', required=True)
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_batch_bq', required=True)
    parser.add_argument('--output_stream_bq', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--job_name', required=True)

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        job_name=args.job_name
    )

    with beam.Pipeline(options=options) as p:

        # Batch pipeline from GCS
        (
        p
         | 'Read Batch CSV' >> ReadFromText(args.input_batch, skip_header_lines=1)
         | 'Parse Batch Records' >> beam.Map(parse_batch_csv)
         | 'Write Batch to BQ' >> WriteToBigQuery(
                args.output_batch_bq,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.FILE_LOADS
            )
        )

        # Stream pipeline from Pub/Sub
        (
        p
         | 'Read PubSub Stream' >> ReadFromPubSub(subscription=args.input_subscription)
         | 'Window Stream Data' >> beam.WindowInto(FixedWindows(60))
         | 'Parse Stream Records' >> beam.Map(parse_stream_json)
         | 'Write Stream to BQ' >> WriteToBigQuery(
                args.output_stream_bq,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STREAMING_INSERTS
            )
        )

if __name__ == '__main__':
    run()
