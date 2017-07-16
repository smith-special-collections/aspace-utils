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
eadid_mapping = JSON.parse(IO.read(File.expand_path(ARGV.shift)))

resources.each do |batch|
  spsh = batch.first
  if spsh['ead_id'] &&
     (ead = client.resource(repo_id: $config['repositories']['mnsss'], id: eadid_mapping[spsh['ead_id']]))

    %w|title finding_aid_title id_0 publish restrictions
       repository_processing_note finding_aid_status finding_aid_note|.each do |direct_field|
      ead[direct_field] = spsh[direct_field] if spsh.key? direct_field
    end


    # Direct array children that should be appended if present in spsh
    %w|revision_statements deaccessions external_documents rights_statements|.each do |append_field|
      ead[append_field] |= spsh[append_field] if spsh.key? append_field
    end

    # Some classes of notes should be appended if in spsh
    auto_append = %w|accruals accessrestrict RestrictedSpecColl RestrictedCurApprSpecColl userestrict|
    if spsh['notes'].any? { |note| auto_append.include? note['type'] }
      ead['notes'] |= spsh['notes'].select { |note| auto_append.include? note['type'] }
    end

    client.queue_update(ead)
  else
    client.queue_json(batch, $config['repositories']['mnsss'], "#{batch[0]['id_0']}:#{batch[0]['ead_id']}")
  end
end

client.run

ingest_logger.info { "OK: #{client.successes} FAIL: #{client.failures} TOTAL: #{client.total}" }
ingest_logger.info { "END INGEST" }

client.close
