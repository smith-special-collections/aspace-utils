#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'securerandom'
require 'date'

class ResourceLoader
  include Enumerable

  # Direct Field mapping (i.e. just copy if present)
  DFM = {
    'finding_aid_title'=>1,
    'title' => 1,
    'id_0'=>2,
    'ead_id'=>3,
    'level'=>4,
    'resource_type'=>5,
    'language'=>6,
    'repository_processing_note'=>9,
    'finding_aid_status'=>12,
    'finding_aid_note'=>13
  }
  # Boolean Field Mapping (1 -> true, 0 -> false)
  BFM = {
    'publish' => 7,
    'restrictions'=>8
  }
  # Extent Field Mapping
  EFM = {
    'portion' => 14,
    'number' => 15,
    'extent_type' => 16,
    'container_summary' => 17
  }
  # Revision Statement Field Mapping
  RSFM = {
    'description' => 19
  }

  # Inclusive Date Field Mapping
  IDateFM = {
    'date_type' => 20,
    'label' => 21,
    'certainty' => 22,
    'expression' => 23,
    'begin' => 24,
    'end' => 25
  }

  # Bulk Date Field Mapping
  BDateFM = {
    'date_type' => 26,
    'label' => 27,
    'expression' => 28
  }

  # Deaccession Field Mapping
  DeaccFM = {
    'scope' => 29,
    'description' => 30,
    'disposition' => 31,
    'date_type' => 32,
    'label' => 33,
    'expression' => 34,
    'begin' => 35
  }

  # Accrual mappings
  AccrualIntFM = {
    'type' => 36,
    'content' => 37,
    'publish' => 38
  }

  AccrualExtFM = {
    'type' => 39,
    'content' => 40,
    'publish' => 41
  }

  # Bioghist
  BioghistFM = {
    'content' => 42,
    'publish' => 43
  }

  # Conditions governing access
  CGAFM = {
    'type' => 44,
    'content' => 45,
    'local_access_restriction_type' => 46,
    'publish' => 47
  }

  # Condition Governing Access: SSC Flavor
  CGASSCFM = {
    'type' => 48,
    'content' => 49,
    'end' => 50,
    'local_access_restriction_type' => 51,
    'publish' => 52
  }

  # Condition governing use
  CGUFM = {
    'type' => 53,
    'content' => 54,
    'publish' => 55
  }

  # Extistence and Locations of copies, originals
  ExtLocCopFM = {
    'type' => 56,
    'content' => 57,
    'publish' => 58
  }

  ExtLocOrigFM = {
    'type' => 59,
    'content' => 60,
    'publish' => 61
  }

  # Donor note
  GenDonorFM = {
    'type' => 62,
    'label' => 63,
    'content' => 64,
    'publish' => 65
  }

  # General note
  GenNoteFM = {
    'type' => 66,
    'content' => 67,
    'publish' => 68
  }

  # Immediate Source of Acquisition
  ImmAcqFM = {
    'type' => 69,
    'content' => 70,
    'publish' => 71
  }

  # Language of Materials
  LangMatFM = {
    'type' => 72, # ignored, ss doesn't match aspace?
    'content' => 73
  }

  # Other Finding Aids
  OtherFAIDFM = {
    'type' => 74,
    'content' => 75,
    'publish' => 76
  }

  # Preferred Citation
  PrefCiteFM = {
    'type' => 77,
    'content' => 78,
    'publish' => 79
  }

  # Related Materials
  RelMatFM = {
    'type' => 80,
    'content' => 81,
    'publish' => 82
  }

  # Scope and Content
  ScopContFM = {
    'type' => 83,
    'content' => 84,
    'publish' => 85
  }

  # Collection Management
  PPlanFM = {
    'processing_hours_per_foot_estimate' => 86,
    'processing_plan' => 87,
    'processing_priority' => 88,
    'processing_funding_source' => 89,
    'processors' => 90,
    'processing_status' => 91
  }

  # Location of donor files
  LDFExtDocFM = {
    'title' => 92,
    'location' => 93,
    'publish' => 94
  }

  # Location of electronic files
  LEFExtDocFM = {
    'title' => 95,
    'location' => 96,
    'publish' => 97
  }

  # Location of Finding Aid File
  LFAFExtDocFM = {
    'title' => 98,
    'location' => 99,
    'publish' => 100
  }

  #ASK: Rights statement looks like it needs discussion
  RightsStmtFM = {
    'rights_type' => 118,
    'status' => 120,
    'jurisdiction' => 121,
    'end_date' => 123
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

  # There are a bunch of note fields with similar structure, try and handle them generically
  def handle_note!(fm, row, notes)
    if row[fm['content']]
      note = {
        'jsonmodel_type' => 'note_multipart',
        'type' => row[fm['type']],
        'subnotes' => [
          {
            'jsonmodel_type' => 'note_text',
            'publish' => row[fm['publish']] == '1',
            'content' => row[fm['content']]
          }
        ]
      }

      note['label'] = row[fm['label']] if fm['label']

      if fm['local_access_restriction_type']
        note['rights_restriction'] = {
          'jsonmodel_type' => 'rights_restriction',
          'publish' => row[fm['publish']] == '1',
          'local_access_restriction_type' => row[fm['local_access_restriction_type']]
        }
        note['rights_restriction']['end'] = row[fm['end']] if row[fm['end']]
      end
      notes << note
    end
  end

  # External documents! They're different!
  def handle_external_document!(fm, row, docs)
    if row[fm['title']] && row[fm['location']] && row[fm['publish']]
      doc = {
        'jsonmodel_type' => 'external_document',
        'title' => row[fm['title']],
        'location' => row[fm['location']],
        'publish' => row[fm['publish']] == '1'
      }
      docs << doc
    end
  end


  def process_row(row)
    # Handle fields that are just set directly on resource object
    resource = DFM.map do |field, idx|
      [field, row[idx]]
    end.to_h

    # Default @level
    unless resource['level']
      resource['level'] = 'collection'
    end

    # Boolean fields directly on resource object
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


    resource.merge!(bool)

    resource['jsonmodel_type'] = 'resource'

    # Establish temporary resource ID
    temp_id = resource['uri'] = "/repositories/import/resources/import_#{SecureRandom::uuid}"

    # Extent (defaults to 1 collection)
    resource['extents'] = []
    if row[EFM['portion']] && row[EFM['number']] && row[EFM['extent_type']]
      resource['extents'] << {
        'portion' => row[EFM['portion']],
        'number' => row[EFM['number']],
        'extent_type' => row[EFM['extent_type']],
        'container_summary' => row[EFM['container_summary']],
        'jsonmodel_type' => 'extent'
      }
    else
      resource['extents'] << {
        'portion' => 'whole',
        'number' => '1',
        'extent_type' => 'collection',
        'jsonmodel_type' => 'extent'
      }
    end

    # Revision statement
    if row[RSFM['description']]
      resource['revision_statements'] = []
      resource['revision_statements'] << {
        'date' => DateTime.now.iso8601,
        'description' => row[RSFM['description']],
        'jsonmodel_type' => 'revision_statement'
      }
    end

    # Inclusive date on resource
    if row[IDateFM['date_type']]
      resource['dates'] ||= []
      resource['dates'] << {
        'date_type' => row[IDateFM['date_type']],
        'label' => row[IDateFM['label']],
        'certainty' => row[IDateFM['certainty']],
        'expression' => row[IDateFM['expression']],
        'begin' => row[IDateFM['begin']],
        'end' => row[IDateFM['end']]
      }
    end

    # Bulk date on resource
    if row[BDateFM['date_type']]
      resource['dates'] ||= []
      resource['dates'] << {
        'date_type' => row[IDateFM['date_type']],
        'label' => row[IDateFM['label']],
        'expression' => row[IDateFM['expression']]
      }
    end

    # Deaccession
    if row[DeaccFM['scope']]
      resource['deaccessions'] = []
      resource['deaccessions'] << {
        'scope' => row[DeaccFM['scope']],
        'description' => row[DeaccFM['description']],
        'disposition' => row[DeaccFM['disposition']],
        'date' => {
          'date_type' => row[DeaccFM['date_type']],
          'label' => row[DeaccFM['label']],
          'expression' => row[DeaccFM['expression']]
        }
      }
    end

    notes = resource['notes'] = []

    handle_note!(AccrualIntFM, row, notes)
    handle_note!(AccrualExtFM, row, notes)

    # ASK: type note_bioghist exists, but can't be attached to resource, just agent - for now, ingest as note
    # Bioghist
    if row[BioghistFM['content']]
      notes << {
        'jsonmodel_type' => 'note_multipart',
        'type' => 'bioghist',
        'label' => 'Biographical Note',
        'subnotes' => [
          {
            'jsonmodel_type' => 'note_text',
            'publish' => row[BioghistFM['publish']] == '1',
            'content' => row[BioghistFM['content']]
          }
        ]
      }
    end



# Ask Jasmin ASAP, need to talk this over
#    handle_note!(CGAFM, row, notes)
#    handle_note!(CGASSCFM, row, notes)
    handle_note!(CGUFM, row, notes)
    handle_note!(ExtLocCopFM, row, notes)
    handle_note!(ExtLocOrigFM, row, notes)
    handle_note!(GenDonorFM, row, notes)
    handle_note!(GenNoteFM, row, notes)
    handle_note!(GenNoteFM, row, notes)
    handle_note!(ImmAcqFM, row, notes)

    if row[LangMatFM['content']]
      notes << {
        'jsonmodel_type' => 'note_singlepart',
        'type' => 'langmaterial',
        'content' => [row[LangMatFM['content']]]
      }
    end

    handle_note!(OtherFAIDFM, row, notes)
    handle_note!(PrefCiteFM, row, notes)
    handle_note!(RelMatFM, row, notes)
    handle_note!(ScopContFM, row, notes)

    # Linked - included in final results immediately before return of fn
    processing_plan = {
      'jsonmodel_type' => 'collection_management',
      'processing_hours_per_foot_estimate' => row[PPlanFM['processing_hours_per_foot_estimate']],
      'processing_plan' => row[PPlanFM['processing_plan']],
      'processing_priority' => row[PPlanFM['processing_priority']],
      'processing_funding_source' => row[PPlanFM['processing_funding_source']],
      'processors' => row[PPlanFM['processors']],
      'processing_status' => row[PPlanFM['processing_status']],
      'resource' => {
        'ref' => temp_id
      },
      'uri' => "/repositories/import/collection_management/import_#{SecureRandom::uuid}"
    }

    external_documents = resource['external_documents'] = []
    handle_external_document!(LDFExtDocFM, row, external_documents)
    handle_external_document!(LEFExtDocFM, row, external_documents)
    handle_external_document!(LFAFExtDocFM, row, external_documents)

    resource['rights_statements'] = []
    if row[RightsStmtFM['rights_type']]
      resource['rights_statements'] << {
        'jsonmodel_type' => 'rights_statement',
        'rights_type' => row[RightsStmtFM['rights_type']],
        'status' => row[RightsStmtFM['status']],
        'jurisdiction' => row[RightsStmtFM['jurisdiction']],
        'end_date' => row[RightsStmtFM['end_date']]
      }
    end

    results = [resource]
    results << processing_plan if processing_plan
    results
  end

end
