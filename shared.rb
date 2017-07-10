require 'yaml'
require 'json'
require 'bundler'
Bundler.require(:default)

raise "No config" unless File.exist?('config.yml')
$config = YAML.safe_load(IO.read('config.yml'))

class IngestLogger
  def initialize(path)
    @file = File.open(path, 'a')
    @lock = Mutex.new
  end

  %I|debug info warn error|.each do |level|
    define_method level, ->(text = nil, &block) do
      if block
        text = block.yield
      end

      @lock.synchronize do
        @file.write("#{DateTime.now.to_s.sub(/-04:00\z/, '')} [#{level.to_s.upcase}] #{text}\n")
        @file.flush
      end
    end
  end

  def close
    @file.close
  end
end

class ErrorResponseLogger
  @@start_marker = '<<<<<<<<<<<<<<<<<<<<<<<<<<<'
  @@end_marker   = '>>>>>>>>>>>>>>>>>>>>>>>>>>>'

  def initialize(path)
    @file = File.open(path, 'a')
    @lock = Mutex.new
  end

  def debug(phase, fname, object)
    is_error = object.is_a? StandardError
    @lock.synchronize do
      @file.puts "#{is_error ? "Error" : "Response"} for '#{fname}' at #{DateTime.now.to_s.sub(/-04:00\z/, '')} [#{phase.to_s.upcase}]"
      @file.puts @@start_marker
      @file.puts (is_error ? object.backtrace.join("\n") : object.body)
      @file.puts @@end_marker
      @file.flush
    end
  end

  def close
    @file.close
  end
end

class AspaceIngester
  @@auth = nil
  attr_reader :total, :successes, :eadid_id_mapping

  def initialize(ingest_logger = IngestLogger.new("spsh_ingestlog.log"), error_logger = ErrorResponseLogger.new('spsh_error_responses'))
    @ingest_logger = ingest_logger
    @error_logger = error_logger
    @hydra = Typhoeus::Hydra.new(max_concurrency: $config.fetch('max_concurrency', 4))
    @base_uri = "#{$config['backend_uri']}"
    @total = 0
    @successes = 0
    @eadid_id_mapping = {}
    @maplock = Mutex.new
    @succlock = Mutex.new
    @totlock = Mutex.new
  end

  def parse_json(txt)
    JSON.parse(txt)
  rescue JSON::ParserError => e
    nil
  end

  def failures
    @total - @successes
  end


  def authorize
    res = Typhoeus.post(URI.join(@base_uri, "/users/#{$config.fetch('username', 'admin')}/login"),
                        params: {password: $config['password']})
    if res.code == 200
      sess = parse_json(res.body)
      token = sess['session']
      return (@@auth = token) if token
    end

    @ingest_logger.error { "Failed to aquire auth" }
    raise "Failed to aquire auth"
  end

  def resource(repo_id:, id:)
    res = Typhoeus.get(URI.join(@base_uri, "/repositories/#{repo_id}/resources/#{id}"),
                       headers: {'X-ArchivesSpace-Session' => @@auth || authorize,
                                 'Content-type' => 'application/json; charset=UTF-8'})
    if res.code == 200
      parse_json(res.body)
    else
      nil
    end
  end

  def queue_update(data)
    @totlock.synchronize do
      @total += 1
    end

    identifier = "#{data['uri'][/\d+$/]}:#{data['ead_id']}"

    update_req = Typhoeus::Request.new(
      URI.join(@base_uri, data['uri']),
      method: :post,
      accept_encoding: "gzip",
      headers: {
        'X-ArchivesSpace-Session' => @@auth,
        'Content-Type' => 'application/json; charset=UTF-8'
      },
      body: JSON.dump(data))
    update_req.on_complete do |res|
      if res.code != 200
        @ingest_logger.warn { "Update of '#{identifier}' failed with code '#{res.code}', body of response is in 'error_responses'" }
        @error_logger.debug(:upload, data['ead_id'], res)
        nil
      elsif (payload = parse_json(res.body)) &&
             payload.key?('errors') &&
             !payload['errors'].empty?
        @ingest_logger.warn { "Update of '#{identifier}' failed with error '#{payload['errors']}'" }
        nil
      else
        @ingest_logger.info { "Update of '#{identifier}' succeeded"}
        @succlock.synchronize do
          @successes += 1
        end
      end
    end
    @hydra.queue(update_req)
  end


  def queue_json(data, repo_id, identifier, from_ead = false)
    unless from_ead
      @totlock.synchronize do
        @total += 1
      end
    end

    upload_req = Typhoeus::Request.new(
      URI.join(@base_uri, "/repositories/#{repo_id}/batch_imports"),
      method: :post,
      accept_encoding: "gzip",
      headers: {
        'X-ArchivesSpace-Session' => @@auth,
        'Content-type' => 'application/json; charset=UTF-8'
      },
      body: JSON.dump(data))
    upload_req.on_complete do |res|
      if res.code != 200
        @ingest_logger.warn { "Upload of '#{identifier}' failed with code '#{res.code}', body of response is in 'error_responses'" }
        @error_logger.debug(:upload, identifier, res)
        nil
      elsif (payload = parse_json(res.body)) &&
            payload.last.key?('errors') &&
            !payload.last['errors'].empty?
        @ingest_logger.warn { "Upload of '#{identifier}' failed with error '#{payload.last['errors']}'" }
        nil
      else
        @ingest_logger.info { "Upload of '#{identifier}' succeeded"}
        @succlock.synchronize do
          @successes += 1
        end

        if from_ead
          @maplock.synchronize do
            eadid = identifier[/(?<=\/)[^\/.]+(?=.xml)/]
            # This is horrible, but then so is the output from the ingester
            # payload is an array of "status" - e.g. "we're this far into the ingest, saving blah de bloo, etc." followed by a "result".
            # Result is a map of form {"saved" => {}}, where the hash within which...
            # is a map of entries of the form temp_ingest_id => [final_url, database_id]

            # So getting the db id looks like this.
            # Assumes a SINGLE resource is being imported at a time, and that temp identifiers are formed "/repositories/import/resources/SOMETHING"
            @eadid_id_mapping[eadid] = payload.last['saved'].select {|k,v| k[%r|/repositories/import/resources/|]}.values.last.last
          end
        end
        payload
      end
    end
    @hydra.queue(upload_req)
  end

  def queue_ead(fname)
    repo_id = $config['repositories'][File.basename(fname)[/^\D+/]]
    @totlock.synchronize do
      @total += 1
    end

    json_req = Typhoeus::Request.new(
      URI.join(@base_uri, '/plugins/jsonmodel_from_format/resource/ead'),
      method: :post,
      accept_encoding: "gzip",
      headers: {
        'X-ArchivesSpace-Session' => @@auth,
        'Content-Type' => 'text/xml; charset=UTF-8'
      },
      body: IO.read(fname)
    )
    json_req.on_complete do |res|
      success = if res.code != 200
               @ingest_logger.warn { "Conversion of '#{fname}' failed with code '#{res.code}', body of response is in 'error_responses'" }
               @error_logger.debug(:conversion, fname, res)
               nil
             elsif (payload = parse_json(res.body)) && payload.is_a?(Hash)
               @ingest_logger.warn { "Conversion of '#{fname}' failed with error '#{payload['error']}'" }
               nil
             else
               @ingest_logger.info { "Conversion of '#{fname}' succeeded" }
               payload
             end
      if success
        queue_json(success, repo_id, fname, true)
      end
    end
    @hydra.queue(json_req)
  end

  def run
    @hydra.run
  end

  def close
    @ingest_logger.close
    @error_logger.close
  end
end
