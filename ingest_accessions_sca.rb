#!/usr/bin/env ruby
require './shared'
require './accession_parser'

ingest_logger = IngestLogger.new('ingestlog.scaacc.log')
error_logger = IngestLogger.new('errorlog.scaacc.log')

ingest_logger.info { "Start of processing" }

client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize
repo_id = $config['repositories'][ARGV[1]]

# HACK: because event requires an agent, establish catchall agents by repo
agent_data = {
  jsonmodel_type: 'agent_corporate_entity',
  names: [
    {
      jsonmodel_type: 'name_corporate_entity',
      primary_name: 'Smith College Archives',
      sort_name: 'Smith College Archives',
      source: 'local'
    }
  ],
  role: [
    "implementer"
  ]
}

agent_uri = client.agent(agent_data)
classifications = client.classifications_map(repo_id)
accessions = AccessionLoader.new(File.expand_path(ARGV[0]), repo_id, client, agent_uri, classifications: classifications)

accessions.each do |batch|
  client.queue_json(batch, repo_id, "#{batch[0].values_at('id_0', 'id_1', 'id_2').compact.join('-')}")
end

client.run

ingest_logger.info { "OK: #{client.successes} FAIL: #{client.failures} TOTAL: #{client.total}" }
ingest_logger.info { "END INGEST" }

client.close
