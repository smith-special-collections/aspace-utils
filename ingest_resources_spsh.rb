#!/usr/bin/env ruby
require './shared'
require './resource_parser'

ingest_logger = IngestLogger.new('ingestlog.spsh.log')
error_logger = ErrorResponseLogger.new('error_log.spsh')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

resources = ResourceLoader.new(File.expand_path(ARGV.shift))
resources.each do |resource|
  client.queue_json(JSON.dump([resource]), 3 ,"#{resource['id_0']}:#{resource['ead_id']}")
end

client.run

ingest_logger.info { "OK: #{client.successes} FAIL: #{client.failures} TOTAL: #{client.total}" }
ingest_logger.info { "END INGEST" }

client.close