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
agent_uris = []
resources = []

agents.each.with_index do |agent, idx|
  if agent[:linked_resource_id_n]
    resources[idx] = agent[:linked_resource_id_n]
    agent.delete :linked_resource_id_n
  end
  agent_uris << client.agent(agent)
end

client.run

# id_n -> [agent_uri, ...]
idn_2_uris = agent_uris.
             map.
             with_index.
             group_by {|s, i| resources[i]}.map {|k,v| [k, v.map(&:first)]}.to_h

idn_2_uris.each_pair do |id_n, uris|
  req = client.resource(id_n: id_n)
  record = JSON.parse(req['json'])
  uris.each do |uri|
    unless record['linked_agents'] && record['linked_agents'].map {|agent|
             agent['role'] == 'subject' && agent['ref'] == uri
           }.any?
      record['linked_agents'] << {'role' => 'subject', 'ref' => uri}
    end
  end
  client.queue_update(record)
  client.run
end

ingest_logger.info { "FINISHED INGEST" }

client.close
