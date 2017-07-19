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

agent_uris = []
resources = []

agents.each.with_index do |agent, idx|
  if agent[:linked_resource_id_n]
    resources[idx] = client.resource(id_n: agent[:linked_resource_id_n])
    agent.delete :linked_resource_id_n
  end
  agent_uris << client.agent(agent)
end

client.run

agent_uris.each.with_index do |uri, idx|
  if resources[idx]
    record = JSON.parse(resources[idx]['json'])
    unless record['linked_agents'] && record['linked_agents'].map {|agent|
             agent['role'] == 'subject' && agent['ref'] == uri
           }.any?
      record['linked_agents'] << {'role' => 'subject', 'ref' => uri}
      client.queue_update(record)
    end
  end
end

ingest_logger.info { "FINISHED INGEST" }

client.close
