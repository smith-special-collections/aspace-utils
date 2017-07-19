#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class CorporateEntityLoader
  include Enumerable

  # Boolean Mapping (1 -> true, 0 -> false)
  BFM = {
    publish: 1
  }

  DateFM = {
    expression: 5,
    date_type: 6,
    begin: 7,
    end: 8,
    certainty: 9
  }

  NameFM = {
    authority_id: 10,
    source: 11,
    primary_name: 12,
  }

  ContactA_FM = {
    name: 13,
    address_1: 14,
    address_2: 15,
    address_3: 16,
    city: 17,
    region: 18,
    country: 19,
    post_code: 20,
    email: 21,
    note: 22
  }

  # Telephone fields
  Phone1_FM = {
    number_type: 23,
    number: 24
  }

  Phone2_FM = {
    number_type: 25,
    number: 26
  }

  Phone3_FM = {
    number_type: 27,
    number: 28
  }

  Phone4_FM = {
    number_type: 29,
    number: 30
  }

  ContactB_FM = {
    name: 31,
    address_1: 32,
    address_2: 33,
    address_3: 34,
    city: 35,
    region: 36,
    country: 37,
    post_code: 38,
    email: 39,
    note: 40
  }

  PhoneForB_FM = {
    number_type: 41,
    number: 42
  }

  BioghistAFM = {
    label: 44,
    publish: 45,
    note_text: 46
  }

  BioghistBFM = {
    label: 48,
    publish: 49,
    note_text: 50
  }

  ExtDocFM = {
    title: 51,
    location: 52,
    publish: 53
  }

  LinkedResourceFM = {
    id_0: 54,
    id_1: 55,
    id_2: 56
  }

  def initialize(fpath)
    csv = CSV.open(fpath)
    @contents = csv.to_a
    @source_map = @contents[0][1..-1]
    @records = @contents[4..-1]
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

  def handle_bioghist!(fm, row, agent)
    if row[fm[:note_text]]
      agent[:notes] ||= []
      agent[:notes] << {
        jsonmodel_type: 'note_bioghist',
        label: row[fm[:label]],
        publish: row[fm[:publish]] == '1',
        subnotes: [
          {
            jsonmodel_type: 'note_text',
            publish: row[fm[:publish]] == '1',
            content: row[fm[:note_text]]
          }
        ]
      }.reject {|k,v| v.nil?}
    end
  end

  def process_row(row)
    agent = {
      jsonmodel_type: 'agent_corporate_entity',
      publish: row[BFM[:publish]] == '1'
    }

    if row[DateFM[:expression]]
      agent[:dates_of_existence] = [
        DateFM.map {|k,v| [k, row[v]] }.to_h.merge(jsonmodel_type: 'date',
                                                   label: 'existence',
                                                   date_type: 'inclusive')
      ]
    end

    agent[:names] = []
    if row[NameFM[:primary_name]]
      agent[:names] << NameFM.map {|k, v|
        [k, row[v]]
      }.to_h.merge(jsonmodel_type: 'name_corporate_entity',
                   sort_name: row[NameFM[:primary_name]])
    end

    if row[ContactA_FM[:name]]
      agent[:agent_contacts] ||= []
      contact = ContactA_FM.map {|k,v|
        [k,row[v]]
      }.to_h.merge(jsonmodel_type: 'agent_contact')
      [Phone1_FM, Phone2_FM, Phone3_FM, Phone4_FM].each do |fm|
        if row[fm[:number]]
          contact[:telephones] ||= []
          contact[:telephones] << fm.map {|k,v|
            [k, row[v]]
          }.to_h.merge(jsonmodel_type: 'telephone')
        end
      end
      agent[:agent_contacts] << contact
    end

    if row[ContactB_FM[:name]]
      agent[:agent_contacts] ||= []
      contact = ContactA_FM.map {|k,v|
        [k,row[v]]
      }.to_h.merge(jsonmodel_type: 'agent_contact')
      if row[PhoneForB_FM[:number]]
        contact[:telephones] ||= []
        contact[:telephones] << PhoneForB_FM.map {|k,v|
          [k, row[v]]
        }.to_h.merge(jsonmodel_type: 'telephone')
      end
      agent[:agent_contacts] << contact
    end

    handle_bioghist!(BioghistAFM, row, agent)
    handle_bioghist!(BioghistBFM, row, agent)

    if row[ExtDocFM[:location]]
      agent[:external_documents] ||= []
      doc = ExtDocFM.reject{|k,_| k == :publish}.map {|k,v|
        [k, row[v]]
      }.to_h.merge(jsonmodel_type: 'external_document',
                   publish: row[ExtDocFM[:publish]] == '1')
      doc[:title] = 'website' unless doc[:title] # default if missing
      agent[:external_documents] << doc
    end

    if row[LinkedResourceFM[:id_0]]
      agent[:linked_resource_id_n] = row.values_at(*LinkedResourceFM.values)
    end

    agent
  end
end
