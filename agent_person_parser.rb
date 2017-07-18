#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class AgentLoader
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
    name_order: 12,
    primary_name: 13,
    rest_of_name: 14,
    suffix: 15
  }

  NickNameFM = {
    authority_id: 16,
    source: 17,
    name_order: 18,
    primary_name: 19
  }

  ContactA_FM = {
    name: 20,
    address_1: 21,
    address_2: 22,
    address_3: 23,
    city: 24,
    region: 25,
    country: 26,
    post_code: 27,
    email: 28,
    note: 29
  }

  # Telephone fields
  Phone1_FM = {
    number_type: 30,
    number: 31
  }

  Phone2_FM = {
    number_type: 32,
    number: 33
  }

  Phone3_FM = {
    number_type: 34,
    number: 35
  }

  Phone4_FM = {
    number_type: 36,
    number: 37
  }

  ContactB_FM = {
    name: 38,
    address_1: 39,
    address_2: 40,
    address_3: 41,
    city: 42,
    region: 43,
    country: 44,
    post_code: 45,
    note: 46
  }

  BioghistAFM = {
    label: 48,
    publish: 49,
    note_text: 50
  }

  BioghistBFM = {
    label: 52,
    publish: 53,
    note_text: 54
  }

  ExtDocAFM = {
    title: 55,
    location: 56,
    publish: 57
  }

  ExtDocBFM = {
    title: 58,
    location: 59,
    publish: 60
  }

  LinkedResourceFM = {
    id_0: 61,
    id_1: 62,
    id_2: 63
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
      jsonmodel_type: 'agent_person',
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
      }.to_h.merge(jsonmodel_type: 'name_person',
                   sort_name: row[NameFM[:primary_name]])
    end

    if row[NickNameFM[:primary_name]]
      agent[:names] << NickNameFM.map {|k, v|
        [k, row[v]]
      }.to_h.merge(jsonmodel_type: 'name_person',
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
      agent[:agent_contacts] << ContactA_FM.map {|k,v|
        [k,row[v]]
      }.to_h.merge(jsonmodel_type: 'agent_contact')
    end

    handle_bioghist!(BioghistAFM, row, agent)
    handle_bioghist!(BioghistBFM, row, agent)

    [ExtDocAFM, ExtDocBFM].each do |fm|
      if row[fm[:location]]
        agent[:external_documents] ||= []
        agent[:external_documents] << fm.reject{|k,_| k == :publish}.map {|k,v|
          [k, row[v]]
        }.to_h.merge(jsonmodel_type: 'external_document',
                     publish: row[fm[:publish]] == '1')
      end

    end


    if row[LinkedResourceFM[:id_0]]
      agent[:linked_resource_id_n] = row.values_at(*LinkedResourceFM.values)
    end

    agent
  end
end
