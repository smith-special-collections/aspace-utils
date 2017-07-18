#!/usr/bin/env ruby
require './shared'
require './agent_corporate_entity_parser'

repo_id = $config['repositories'][ARGV[1]]

ingest_logger = IngestLogger.new('ingestlog.agent_corporate_entity.log')
error_logger = ErrorResponseLogger.new('error_log.agent_corporate_entity.log')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

agents = CorporateEntityLoader.new(File.expand_path(ARGV[0]))

agents.each do |agent|
  client.agent(agent)
end

client.run

ingest_logger.info { "FINISHED INGEST" }

client.close
