#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class AccessionAgentLoader
  include Enumerable

  LinkedAccFM = {
    id_0: 0,
    id_1: 1,
    id_2: 2
  }
  AgentFM = {
    role: 3,
    source: 5,
    authority_id: 6,
    primary_name: 7,
    rest_of_name: 8
  }

  def initialize(fpath)
    csv = CSV.open(fpath)
    @contents = csv.to_a
    @records = @contents[1..-1]
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
    agent = {jsonmodel_type: 'agent_person',
             names: [
               {
                 jsonmodel_type: 'name_person',
                 primary_name: row[AgentFM[:primary_name]],
                 sort_name: row[AgentFM[:primary_name]],
                 rest_of_name: row[AgentFM[:rest_of_name]],
                 source: row[AgentFM[:source]],
                 name_order: 'inverted'
               }
             ],
             linked_acc_id_n: row.values_at(*LinkedAccFM.values)
            }
    agent
  end

end
