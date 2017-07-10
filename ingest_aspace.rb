#!/usr/bin/env ruby
require './shared'
ingest_logger = IngestLogger.new('ingestlog.log')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger)

ingest_files = Dir[File.join($config['ingest_dir'], '*.xml')].
               sort.
               select {|f| $config['repositories'][File.basename(f)[/^\D+/]]}
ingest_files.each_slice($config.fetch('batch_size', 20)) do |batch|
  client.authorize
  if batch.count > 0
    batch.each do |fname|
      client.queue_ead(fname)
    end
    client.run
  end
end

ingest_logger.info { "OK: #{client.successes} FAIL: #{client.failures} TOTAL: #{client.total}" }
ingest_logger.info { "END INGEST" }

IO.write("eadids_2_ids.json", client.eadid_id_mapping.to_json)

client.close
