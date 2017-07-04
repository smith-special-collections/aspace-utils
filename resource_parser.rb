#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class ResourceLoader
  include Enumerable

  # Direct Field mapping (i.e. just copy if present)
  DFM = {
    "finding_aid_title"=>1,
    "title" => 1,
    "id_0"=>2,
    "ead_id"=>3,
    "level"=>4,
    "resource_type"=>5,
    "language"=>6,
    "repository_processing_note"=>9,
    "finding_aid_status"=>12,
    "finding_aid_note"=>13
  }
  # Boolean Field Mapping (1 -> true, 0 -> false)
  BFM = {
    "publish" => 7,
    "restrictions"=>8
  }
  # Extent Field Mapping
  EFM = {
    "portion" => 14,
    "number" => 15,
    "extent_type" => 16,
    "container_summary" => 17
  }
  # Revision Statement Field Mapping
  RSFM = {
    "description" => 19
  }

  # Inclusive Date Field Mapping
  IDateFM = {
    "date_type" => 20,
    "label" => 21,
    "certainty" => 22,
    "expression" => 23,
    "begin" => 24,
    "end" => 25
  }

  # Bulk Date Field Mapping
  BDateFM = {
    "date_type" => 26,
    "label" => 27,
    "expression" => 28
  }

  def initialize(fpath)
    csv = CSV.open(fpath)
    @contents = csv.to_a
    @source_map = @contents[0][1..-1]
    @records = @contents[4..-1]
  end

  def each(&block)
    @records.each do |record|
      block.call(process_row(record))
    end
  end

  def [](idx)
    process_row(@records[idx])
  end

  def process_row(row)
    direct = DFM.map do |field, idx|
      [field, row[idx]]
    end.to_h

    unless direct['level']
      direct['level'] = 'collection'
    end

    bool = BFM.map do |field, idx|
      [field, case row[idx]
              when 0
                false
              when 1
                true
              else
                nil
              end]
    end.to_h

    direct.merge!(bool)
    direct['jsonmodel_type'] = "resource"
    direct['uri'] = "import_#{SecureRandom::uuid}"

    if row[EFM['portion']] && row[EFM['number']] && row[EFM['extent_type']]
      direct['extents'] = []
      direct['extents'] << {
        'portion' => row[EFM['portion']],
        'number' => row[EFM['number']],
        'extent_type' => row[EFM['extent_type']],
        'container_summary' => row[EFM['container_summary']],
        'jsonmodel_type' => 'extent'
      }
    end

    if row[RSFM['description']]
      direct['revision_statements'] = []
      direct['revision_statements'] << {
        'date' => DateTime.now.iso8601,
        'description' => row[RSFM['description']],
        'jsonmodel_type' => 'revision_statement'
      }
    end


    if row[IDateFM['date_type']]
      direct['dates'] ||= []
      direct['dates'] << {
        'date_type' => row[IDateFM['date_type']],
        'label' => row[IDateFM['label']],
        'certainty' => row[IDateFM['certainty']],
        'expression' => row[IDateFM['expression']],
        'begin' => row[IDateFM['begin']],
        'end' => row[IDateFM['end']]
      }
    end

    if row[BDateFM['date_type']]
      direct['dates'] ||= []
      direct['dates'] << {
        'date_type' => row[IDateFM['date_type']],
        'label' => row[IDateFM['label']],
        'expression' => row[IDateFM['expression']]
      }
    end
    direct
  end

end
