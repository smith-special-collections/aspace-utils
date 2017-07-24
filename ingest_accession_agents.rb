#!/usr/bin/env ruby
require './shared'
require './accession_agent_parser'

ingest_logger = IngestLogger.new('ingestlog.acc_agent_person.log')
error_logger = ErrorResponseLogger.new('error_log.acc_agent_person.log')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

agents = AccessionAgentLoader.new(File.expand_path(ARGV[0]))
agent_uris = []
accessions = []

agents.each.with_index do |agent, idx|
  if agent[:linked_acc_id_n]
    accessions[idx] = agent[:linked_acc_id_n]
    agent.delete :linked_acc_id_n
  end
  agent_uris << client.agent(agent)
end

client.run

# id_n -> [agent_uri, ...]
idn_2_uris = agent_uris.
             map.
             with_index.
             reject {|uri, idx| !accessions[idx] }.
             group_by {|uri, idx| accessions[idx]}.map {|k,v| [k, v.map(&:first)]}.to_h


idn_2_uris.each_pair do |id_n, uris|
  req = client.accession(id_n: id_n)
  record = JSON.parse(req['json'])
  ingest_logger.info {"Linking #{id_n.join("-")}"}
  if record
    uris.each do |uri|
      unless record['linked_agents'] && record['linked_agents'].map {|agent|
               agent['role'] == 'source' && agent['ref'] == uri
             }.any?
        ingest_logger.info {"URI: #{uri}"}
        record['linked_agents'] << {'role' => 'source', 'ref' => uri}
      end
    end
    client.queue_update(record)
    client.run
  end
end

ingest_logger.info { "FINISHED INGEST" }

client.close
