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
    'id_1' => 3,
    'id_2' => 4,

    'ead_id'=>5,
    'level'=>6,
    'resource_type'=>7,
    'language'=>8,
    'repository_processing_note'=>11,
    'finding_aid_status'=>14,
    'finding_aid_note'=>15
  }
  # Boolean Field Mapping (1 -> true, 0 -> false)
  BFM = {
    'publish' => 9,
    'restrictions'=>10
  }
  # Extent Field Mapping
  EFM = {
    'portion' => 16,
    'number' => 17,
    'extent_type' => 18,
    'container_summary' => 19
  }
  # Revision Statement Field Mapping
  RSFM = {
    'description' => 21
  }

  # Inclusive Date Field Mapping
  IDateFM = {
    'date_type' => 22,
    'label' => 23,
    'certainty' => 24,
    'expression' => 25,
    'begin' => 26,
    'end' => 27
  }

  # Bulk Date Field Mapping
  BDateFM = {
    'date_type' => 28,
    'label' => 29,
    'expression' => 30
  }

  # Deaccession Field Mapping
  DeaccFM = {
    'scope' => 31,
    'description' => 32,
    'disposition' => 33,
    'date_type' => 34,
    'label' => 35,
    'expression' => 36,
    'begin' => 37
  }

  # Accrual mappings
  AccrualIntFM = {
    'type' => 38,
    'content' => 39,
    'publish' => 40
  }

  AccrualExtFM = {
    'type' => 41,
    'content' => 42,
    'publish' => 43
  }

  # Bioghist
  BioghistFM = {
    'content' => 44,
    'publish' => 45
  }

  # Conditions governing access
  CGAFM = {
    'type' => 46,
    'content' => 47,
    'local_access_restriction_type' => 48,
    'publish' => 49
  }

  # Condition Governing Access: SSC Flavor
  CGASSCFM = {
    'type' => 50,
    'content' => 51,
    'restriction_end_date' => 52,
    'local_access_restriction_type' => 53,
    'publish' => 54
  }

  # Condition governing use
  CGUFM = {
    'type' => 55,
    'content' => 56,
    'publish' => 57
  }

  # Extistence and Locations of copies, originals
  ExtLocCopFM = {
    'type' => 58,
    'content' => 59,
    'publish' => 60
  }

  ExtLocOrigFM = {
    'type' => 61,
    'content' => 62,
    'publish' => 63
  }

  # Donor note
  GenDonorFM = {
    'type' => 64,
    'label' => 65,
    'content' => 66,
    'publish' => 67
  }

  # General note
  GenNoteFM = {
    'type' => 68,
    'content' => 69,
    'publish' => 70
  }

  # Immediate Source of Acquisition
  ImmAcqFM = {
    'type' => 71,
    'content' => 72,
    'publish' => 73
  }

  # Language of Materials
  LangMatFM = {
    'type' => 74, # ignored, ss doesn't match aspace?
    'content' => 75
  }

  # Other Finding Aids
  OtherFAIDFM = {
    'type' => 76,
    'content' => 77,
    'publish' => 78
  }

  # Preferred Citation
  PrefCiteFM = {
    'type' => 79,
    'content' => 80,
    'publish' => 81
  }

  # Related Materials
  RelMatFM = {
    'type' => 82,
    'content' => 83,
    'publish' => 84
  }

  # Scope and Content
  ScopContFM = {
    'type' => 85,
    'content' => 86,
    'publish' => 87
  }

  # Collection Management
  PPlanFM = {
    'processing_hours_per_foot_estimate' => 88,
    'processing_plan' => 89,
    'processing_priority' => 90,
    'processing_funding_source' => 91,
    'processors' => 92,
    'processing_status' => 93
  }

  # Location of donor files
  LDFExtDocFM = {
    'title' => 94,
    'location' => 95,
    'publish' => 96
  }

  # Location of electronic files
  LEFExtDocFM = {
    'title' => 97,
    'location' => 98,
    'publish' => 99
  }

  # Location of Finding Aid File
  LFAFExtDocFM = {
    'title' => 100,
    'location' => 101,
    'publish' => 102
  }

  # License Rights statement
  # bespoke bc fewer and diff fields
  RSLicenseFM = {
    'rights_type' => 103,
    'license_identifier_terms' => 104
  }

  # Three more-or-less "regular" rights statements
  RightsStmtAFM = {
    'rights_type' => 105,
    'materials' => 106,
    'ip_status' => 107,
    'jurisdiction' => 108,
    'type_note' => 109,
    'permissions' => 110,
    'end_date' => 111,
    'granted' => 112
  }

  RightsStmtBFM = {
    'rights_type' => 113,
    'materials' => 114,
    'ip_status' => 115,
    'jurisdiction' => 116,
    'type_note' => 117,
    'end_date' => 118,
    'granted' => 119
  }

  RightsStmtCFM = {
    'rights_type' => 120,
    'materials' => 121,
    'ip_status' => 122,
    'jurisdiction' => 123,
    'type_note' => 124,
    'end_date' => 125
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
  def handle_note!(fm, row, notes, temp_id: nil)
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

      if fm['local_access_restriction_type'] && row[fm['local_access_restriction_type']
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

  # Handle rights statements
  def handle_rights_statement!(fm, row, stmts)
    if row[fm['rights_type']]
      rights_stmt = {
        'jsonmodel_type' => 'rights_statement',
      }

      # handle other direct fields on the rights statement
      %w|rights_type license_identifier_terms ip_status jurisdiction end_date|.each do |field|
        rights_stmt[field] = row[fm[field]] if fm[field] && row[fm[field]]
      end
      # handle note_rights_statements
      %w|materials type_note|.each do |field|
        if fm[field] && row[fm[field]]
          notes = rights_stmt['notes'] ||= []
          notes << {
            'jsonmodel_type' => 'note_rights_statement',
            'type' => field,
            'content' => [row[fm[field]]]
          }
        end
      end

      # handle note_rights_statement_acts
      act_types = %w|permissions granted|
      if act_types.any? {|t| fm.key? t}
        acts = rights_stmt['acts'] ||= []
        act = {
          'jsonmodel_type' => 'rights_statement_act',
          'notes' => []
        }
        act_types.each do |field|
          if fm[field] && row[fm[field]]
            act['notes'] << {
              'jsonmodel_type' => 'note_rights_statement_act',
              'type' => field,
              'content' => [row[fm[field]]]
            }
          end
        end
      end

      stmts << rights_stmt
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

    handle_note!(CGAFM, row, notes)
    handle_note!(CGASSCFM, row, notes)
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
    processing_plan = if PPlanFM['processing_plan']
                        {
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
                      end
    external_documents = resource['external_documents'] = []
    handle_external_document!(LDFExtDocFM, row, external_documents)
    handle_external_document!(LEFExtDocFM, row, external_documents)
    handle_external_document!(LFAFExtDocFM, row, external_documents)

    if [RSLicenseFM, RightsStmtAFM, RightsStmtBFM, RightsStmtCFM].map do |fm|
         row[fm['rights_type']]  end.any?
      rights_statements = resource['rights_statements'] = []
      handle_rights_statement!(RSLicenseFM, row, rights_statements)
      handle_rights_statement!(RightsStmtAFM, row, rights_statements)
      handle_rights_statement!(RightsStmtBFM, row, rights_statements)
      handle_rights_statement!(RightsStmtCFM, row, rights_statements)
    end

    results = [resource]
    results << processing_plan if processing_plan
    results
  end

end
