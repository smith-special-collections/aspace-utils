#!/usr/bin/env ruby
require './shared'
require './subject_parser'

repo_id = $config['repositories']['mnsss']

ingest_logger = IngestLogger.new('ingestlog.subjects.log')
error_logger = ErrorResponseLogger.new('error_log.subjects.log')

ingest_logger.info { "Start of Processing" }

ingest_logger.info { "BEGIN INGEST" }
client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

subjects = SubjectLoader.new(File.expand_path(ARGV[0]), repo_id)
subject_uris = []
resources = []

subjects.each.with_index do |subject, idx|
  if subject[:linked_resource_id_n]
    resources[idx] = subject[:linked_resource_id_n]
    subject.delete :linked_resource_id_n
  end
  subject_uris << client.subject(subject)
end

client.run

# id_n -> [subject_uri, ...]
idn_2_uris = subject_uris.
             map.
             with_index.
             group_by {|s, i| resources[i]}.map {|k,v| [k, v.map(&:first)]}.to_h

idn_2_uris.each_pair do |id_n, uris|
  req = client.resource(id_n: id_n)
  record = JSON.parse(req['json'])
  if record
    uris.each do |uri|
      unless record['subjects'] && record['subjects'].map {|subject|
               subject['ref'] == uri
             }.any?
        record['subjects'] << {'ref' => uri}
      end
    end
    client.queue_update(record)
    client.run
  end
end

ingest_logger.info { "FINISHED INGEST" }

client.close
