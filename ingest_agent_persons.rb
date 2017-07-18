#!/usr/bin/env ruby
require './shared'
require './agent_person_parser'

repo_id = $config['repositories'][ARGV[1]]

ingest_logger = IngestLogger.new('ingestlog.agent_person.log')
error_logger = ErrorResponseLogger.new('error_log.agent_person.log')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

agents = AgentLoader.new(File.expand_path(ARGV[0]))
responses = []
resources = []

agents.each.with_index do |agent, idx|
  if agent[:linked_resource_id_n]
    resources[idx] = client.resource(id_n: agent[:linked_resource_id_n])
    agent.delete :linked_resource_id_n
  end
  responses << client.agent(agent)
end

client.run

responses.each.with_index do |agents, idx|
  if resources[idx]
    record = JSON.parse(resources[idx]['json'])
    unless record['linked_agents'] && record['linked_agents'].map {|agent|
         agent['role'] == 'subject' && agent['ref'] == responses[idx]
       }.any?
      record['linked_agents'] << {'role' => 'subject', 'ref' => responses[idx]}
      client.queue_update(record)
    end
  end
end

client.run
