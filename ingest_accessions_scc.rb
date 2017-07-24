#!/usr/bin/env ruby
require './shared'
require './accession_parser'

ingest_logger = IngestLogger.new('ingestlog.sccacc.log')
error_logger = IngestLogger.new('errorlog.sccacc.log')

ingest_logger.info { "Start of processing" }

client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize
repo_id = $config['repositories']['mnsss']

# HACK: because event requires an agent, establish catchall agents by repo
agent_data = {
  jsonmodel_type: 'agent_corporate_entity',
  names: [
    {
      jsonmodel_type: 'name_corporate_entity',
      primary_name: 'Sophia Smith Collection',
      sort_name: 'Sophia Smith Collection',
      authority_id: 'http://id.loc.gov/authorities/names/n50071989',
      source: 'naf'
    }
  ],
  role: [
    "implementer"
  ]
}

agent_uri = client.agent(agent_data)

accessions = AccessionLoader.new(File.expand_path(ARGV[0]), repo_id, client, agent_uri)

accessions.each do |batch|
  client.queue_json(batch, repo_id, "#{batch[0].values_at(:id_0, :id_1, :id_2).compact.join('-')}")
end

client.run

ingest_logger.info { "OK: #{client.successes} FAIL: #{client.failures} TOTAL: #{client.total}" }
ingest_logger.info { "END INGEST" }

client.close
