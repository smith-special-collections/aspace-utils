#!/usr/bin/env ruby
require './shared'
require 'csv'
require 'json'
require 'securerandom'

classifications = []
classification_terms = []
identifier2uri = {}

CSV.open('/home/pobocks/Downloads/sca_classifications_2017-06-05 - Sheet1.csv').
                  to_a[2..-1].
                  each do |row|
  (identifier, title, desc) = row[2..-1]
  parent = identifier.split('.')[0..-2].join('.')
  if parent == "" # classification
    temp_uri = identifier2uri[identifier] = "/repositories/import/classifications/import_#{SecureRandom::uuid}"
    classifications << {
      jsonmodel_type: 'classification',
      identifier: identifier,
      title: title,
      description: desc || "",
      uri: temp_uri,
      publish: true
    }
  else
    classification_uri = identifier2uri[parent[/.*?(?=\.|$)/]]
    temp_uri = identifier2uri[identifier] = "/repositories/import/classification_terms/import_#{SecureRandom::uuid}"
    classification_terms << {
      jsonmodel_type: 'classification_term',
      identifier: identifier,
      title: title,
      description: desc || "",
      uri: temp_uri,
      classification: {ref: classification_uri},
      parent: identifier2uri[parent] == classification_uri ? nil : {ref: identifier2uri[parent]},
      publish: true
    }
  end
end

ingest_logger = IngestLogger.new('ingestlog.sca_class.log')
error_logger = ErrorResponseLogger.new('error_log.sca_class')

ingest_logger.info { "Start Classifications" }

client = AspaceIngester.new(ingest_logger, error_logger)

client.authorize

client.queue_json(classifications | classification_terms, $config['repositories']['manosca'], "classifications")
client.run

ingest_logger.info { "Classifications done" }
client.close
