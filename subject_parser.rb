#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class SubjectLoader
  include Enumerable

  SubjectFM = {
    id_0: 1, # id_n refers to resource
    id_1: 2, # ibid.
    id_2: 3, # ibid.
    source: 4,
    term: 5,
    term_type: 6
  }

  def initialize(fpath, repo_id)
    csv = CSV.open(fpath)
    @contents = csv.to_a
    @source_map = @contents[0][1..-1]
    @records = @contents[4..-1]
    @repo_id = repo_id
  end

  def each(&block)
    return @records.lazy.map {|record| process_row(record)} unless block_given?
    @records.each do |record|
      block.call(process_row(record))
    end
  end

  def [](idx)
    process_row(@records[idx])
  end

  def process_row(row)
    {
      jsonmodel_type: 'subject',
      vocabulary: '/vocabularies/1', # global vocab
      source: row[SubjectFM[:source]],
      used_within_repositories: ["/repositories/#{@repo_id}"],
      uri: "/subjects/import_#{SecureRandom.uuid}",
      terms: [
        {
          jsonmodel_type: 'term',
          vocabulary: '/vocabularies/1',
          term_type: row[SubjectFM[:term_type]],
          term: row[SubjectFM[:term]],
          uri: "/terms/import_#{SecureRandom.uuid}"
        }
      ],
      linked_resource_id_n: row.values_at(*SubjectFM.values_at(:id_0, :id_1, :id_2))
    }
  end

end
