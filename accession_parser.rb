#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class AccessionLoader
  include Enumerable

  # Direct Fields
  DFM = {
    id_0: 1,
    id_1: 2,
    id_2: 3,
    accession_date: 4,
    content_description: 5,
    disposition: 6,
    inventory: 7,
    general_note: 8,
    acquisition_type: 9,
    access_restrictions_note: 13,

  }
  # Boolean Fields
  BFM = {
    restrictions_apply: 10,
    publish: 11,
    access_restrictions: 12
  }

  # Revision statements
  RevisionStmtA_FM = {
    date: 17,
    description: 18
  }

  RevisionStmtB_FM = {
    date: 19,
    description: 20
  }

  # Inclusive/Single Date Field Mapping
  DateFM = {
    label: 21,
    expression: 22,
    date_type: 23,
    begin: 24,
    end: 25,
    certainty: 26
  }

  # Extent
  ExtentFM = {
    portion: 27,
    number: 28,
    extent_type: 29,
    container_summary: 30
  }

  # External Document
  ExternalDocFM = {
    title: 31,
    location: 32
  }

  PPlanFM = {
    processors: 33
  }

  AckSentEventFM = {
    event_type: 34,
    outcome_note: 35,
    label: 37,
    date_type: 38,
    expression: 39,
    begin: 40
  }

  AggSentEventFM = {
    event_type: 41,
    label: 43,
    date_type: 44,
    expression: 45,
    begin: 46
  }

  ResourceIdFM = {
    id_0: 47,
    id_1: 48,
    id_2: 49
  }

  def initialize(fpath, repo_id, client, agent_uri, classifications: nil)
    csv = CSV.open(fpath)
    @contents = csv.to_a
    @records = @contents[4..-1]
    @repo_id = repo_id
    @agent_uri = agent_uri
    @client = client # this is a hack, I want to acvoid maintaining a mapping
    @classifications = classifications
  end

  def each(&block)
    @records.each do |record|
      block.call(process_row(record))
    end
  end

  def [](idx)
    process_row(@records[idx])
  end

  def handle_revision_statement!(fm, row, stmts)
    if row[fm[:description]]
      stmts << {
        date: row[fm[:date]],
        description: row[fm[:description]],
        jsonmodel_type: :revision_statement
      }
    end
  end

  # External documents! They're different!
  def handle_external_document!(fm, row, docs)
    if row[fm[:title]] && row[fm[:location]]
      doc = {
        jsonmodel_type: 'external_document',
        title: row[fm[:title]],
        location: row[fm[:location]],
        publish: false,
      }
      docs << doc
    end
  end

  # event (and nested date field)
  def handle_event!(fm, row, refs, events, temp_uri, resource = nil)
    if row[fm[:event_type]]
      event_temp_uri = "/repositories/import/events/import_#{SecureRandom::uuid}"
      refs << {ref: event_temp_uri}
      event = {
        jsonmodel_type: 'event',
        event_type: row[fm[:event_type]],
        uri: event_temp_uri,
        linked_records: [{ref: temp_uri, role: 'source'}],
        linked_agents: [{ref: @agent_uri, role: 'implementer'}],
        date: {
          label: row[fm[:label]],
          date_type: row[fm[:date_type]],
          expression: row[fm[:expression]]
        }
      }
      event[:outcome_note] = row[fm[:outcome_note]] if fm.key?(:outcome_note) && row[fm[:outcome_note]]
      event[:date][:begin] = row[fm[:begin]] if row[fm[:begin]]

      if resource
        event[:linked_records] << {ref: resource['uri'], role: 'source'}
      end
      events << event
    end
  end

  def process_row(row)
    # If there's an attached resource, find it!

    resource = @client.resource(id_n: ResourceIdFM.values.map {|id_n| row[id_n]}, repo_id: @repo_id)

    # Handle fields set directly on accession
    accession = DFM.map do |field, idx|
      [field, row[idx]]
    end.to_h

    bools = BFM.map do |field, idx|
      [field, case row[idx]
              when 0
                false
              when 1
                true
              else
                nil
              end]
    end.to_h

    accession.merge!(bools)
    accession[:jsonmodel_type] = 'accession'

    class_uri = @classifications ? @classifications[row[ResourceIdFM.values.first]] : nil

    if class_uri
      accession[:classifications] = [{ref: class_uri}]
    end

    temp_uri = accession[:uri] = "/repositories/import/accessions/import_#{SecureRandom::uuid}"

    # Revision statement
    accession[:revision_statements] = []
    accession[:revision_statements] << {
      date: DateTime.now.iso8601,
      description: "Accession record ingested into ArchivesSpace.",
      jsonmodel_type: 'revision_statement'
    }
    handle_revision_statement!(RevisionStmtA_FM, row, accession[:revision_statements])
    handle_revision_statement!(RevisionStmtB_FM, row, accession[:revision_statements])

    # Inclusive or single date
    if row[DateFM[:date_type]]
      accession[:dates] ||= []
      date = {
        date_type: row[DateFM[:date_type]],
        label: row[DateFM[:label]],
        certainty: row[DateFM[:certainty]],
        expression: row[DateFM[:expression]],
      }
      date[:begin] = row[DateFM[:begin]] if row[DateFM[:begin]]
      date[:end] = row[DateFM[:end]] if row[DateFM[:end]]
      accession[:dates] << date
    end

    accession[:extents] = []
    # Extent (defaults to 1 collection)
    if row[ExtentFM[:portion]] && row[ExtentFM[:number]] && row[ExtentFM[:extent_type]]
      accession[:extents] << {
        :portion => row[ExtentFM[:portion]],
        :number => row[ExtentFM[:number]],
        :extent_type => row[ExtentFM[:extent_type]],
        :container_summary => row[ExtentFM[:container_summary]],
        :jsonmodel_type => 'extent'
      }
    end

    external_documents = accession[:external_documents] = []
    handle_external_document!(ExternalDocFM, row, external_documents)

    # Linked - included in final results immediately before return of fn
    processing_plan = if row[PPlanFM[:processors]]
                        {
                          jsonmodel_type: 'collection_management',
                          processors: row[PPlanFM[:processors]],
                          resource: {
                            ref: temp_uri
                          },
                          uri: "/repositories/import/collection_management/import_#{SecureRandom::uuid}"
                        }
                      end

    linked_events = accession[:linked_events] = []
    event_records = []

    handle_event!(AckSentEventFM, row, linked_events, event_records, temp_uri, resource || nil)
    handle_event!(AggSentEventFM, row, linked_events, event_records, temp_uri, resource || nil)

    if resource
      accession[:related_resources] = [{ref: resource['uri']}]
    end

    results = [accession]
    results << processing_plan if processing_plan
    results += event_records if event_records
    results
  end

end
