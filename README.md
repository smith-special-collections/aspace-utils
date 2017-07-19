# ASpace EAD Batch Ingest Scripts

These scripts rely on the presence of the [aspace-jsonmodel-from-format plugin](https://github.com/lyrasis/aspace-jsonmodel-from-format) in your instance of ArchivesSpace.
The plugin converts ead xml to json object model, which allows import into ArchivesSpace. These scripts need not run on the same instance as the aspace instance, we use http post calls to the api (http://your.aspaceinstance:8089/..., for instance)

## Prerequisites
- In your (remote or local) instance of aspace, install the aspace-jsonmodel-from-format plugin. Install info at:
[https://github.com/lyrasis/aspace-jsonmodel-from-format](https://github.com/lyrasis/aspace-jsonmodel-from-format)
- Full set of EAD files need to be in a local, accessible directory (specified in config.yml, an example is provided [here](config.yml.example)
- Ruby 2.3+.  This SHOULD work fine with any Ruby of appropriate version, but fails on JRuby for unknown reasons.
## Installation
- Check out this repository
- Create config.yml file based on config.yml.example
- Install dependencies via Bundler

    ``` shell
    gem install bundler # If not already installed
    cd aspace-utils
    bundle install
    ```

## A Note On Order of Operations
**Smith Specific Info**
In running the various ingesters, there are a set of dependencies to keep in mind.

- The spreadsheet ingest needs products of the EAD ingest (resources in ASpace, `eadids_2_ids.json`)
- The accessions ingest for SCA depends on the classifications ingest for SCA
- The corporate entity agent ingest (source ingests) depends on resources in ASpace in some cases depending on data

Additionally, between any step, care must be taken re: the indexer - resource fetching depends on the index.  You should be able to look at `$ASPACE_ROOT_DIRECTORY/logs/archivesspace.out` to tell when indexing is finished.

I'm not actually sure if this is a complete listing of dependencies, but I AM sure that the following order is safe:


1. `ingest_aspace.rb` - the EAD ingest
2. `ingest_resources_spsh.rb` - the SCC resources spreadsheet.
3. `ingest_classifications_sca.rb` - classifications for SCA - this is actually safe to run at any point, has no dependencies
4. `ingest_accessions_sca.rb` and `ingest_accessions_scc.rb` - the accessions ingests
5. `ingest_agent_persons.rb` and `ingest_corporate_entities.rb` - agent ingests


## Running the EAD ingester
To run the ingester, place you EAD files in the directory specified in your `config.yml`, and then run:

``` shell
bundle exec ingest_aspace.rb
```

If you want to keep an eye on what it's doing, I recommend:

``` shell
watch tail ingestlog.log
```

The ingester populates two log files - `ingestlog.log` and `error_responses`.  It also creates a JSON file with a mapping of eadids to ids.

In general, this has been run under [screen](https://www.gnu.org/software/screen/) to keep this running over long periods of time somewhere it can be kept track of.  General practice:

```shell
rvm use 2.3.3
screen
bundle exec ingest_aspace &
tail -f ingestlog.log
<CTRL-a CTRL-d> # detach from screen
```

Then, to check on progress, you can just `screen -r` to reattach to the screen, and detach again when you want to do something else with `<CTRL-a CTRL-d>`

This is also applicable to the other ingest scripts - just replace name of script/logfile and arguments.

## Non-EAD resource Ingest
`ingest_resources_spsh.rb` takes two arguments - the spreadsheet with the resources, and the `eadids_2_ids.json` file produced by the EAD ingester.

Run as thus:

```shell
rvm use 2.3.3
bundle exec ingest_resources_spsh.rb ~/path/to/my/spreadsheet.csv ./eadids_2_ids.json
```

It will log to `ingestlog.spsh.log` and `error_log.spsh`

## Classifications Ingest
`ingest_classifications_sca.rb` takes one argument, the spreadsheet that contains your classifications listing.

Run as thus:

```shell
rvm use 2.3.3
bundle exec ingest_classifications_sca.rb ~/path/to/my/spreadsheet.csv
```

## Accessions Ingest
`ingest_accessions_scc.rb` and `ingest_accessions_sca.rb` take one argument - the spreadsheet with the accessions.

Run as thus:
```shell
rvm use 2.3.3
bundle exec ingest_accessions_scc.rb ~/path/to/scc/spreadsheet.csv
bundle exec ingest_accessions_sca.rb ~/path/to/sca/spreadsheet.csv
```

## Agent Ingest
`ingest_agent_persons.rb` and `ingest_corporate_entities.rb` are used for various agent spreadsheets.  They come in two varieties: person records, which are things like faculty or alumni records, and corporate_entity or source records. Additionally, some of these resources have associated resource identifiers in the last three columns of their spreadsheets.

These scripts all take two arguments: the spreadsheet with the agent data, and the MARC relator code (the prefix on EAD filenames before the numbers start) for the repository in question.  For SCA this is 'manosca', and for SSC this is 'mnsss'

### Person records

```shell
rvm use 2.3.3
bundle exec ingest_agent_persons.rb ~/path/to/scc/spreadsheet.csv mnsss
bundle exec ingest_agent_persons.rb ~/path/to/sca/spreadsheet.csv manosca
```

### Corporate Records

```shell
rvm use 2.3.3
bundle exec ingest_corporate_entities.rb ~/path/to/scc/spreadsheet.csv mnsss
bundle exec ingest_corporate_entities.rb ~/path/to/sca/spreadsheet.csv manosca
```

## A cautionary note on max_concurrency
This script can do concurrent requests, but versions of ArchivesSpace prior to 1.5.2 had a race condition around creating Subjects/Agents/other shared fields.  If using any prior version, you MUST set max_concurrency to 1.

## Analysis script
There's also an "analyze_logs.rb" script provided, which is only really reliable on the EAD ingest log right now.
It can be used thusly:

```
bundle exec analyze_logs.rb ingestlog.log error_responses > analysis.txt
```

It currently assumes that there is ONE and only ONE set of logs in each of those files - if you want to use the analysis script, you'll need to wipe ingestlog.log and error_responses between runs.

Note that passing more than two arguments will enter the script in interactive mode - you'll be thrown into a pry session with several interesting local variables defined.

| Name | Description| Type |
| ---- | ---------- | ---- |
| upload_failures | Failures to upload resulting from ASpace DB errors/java errors | Hash keyed by approximate cause with values being hashes keyed by eadid |
| four_hundreds | Errors that come from the EAD converter | Hash of errors keyed by eadid |
| five_hundreds | Errors from Apache or Java or gremlins | Hash of errors keyed by eadid |
| by_error | Big ole hash with 4XX, 5XX, and upload failures aggregated by proximate cause | Hash of hashes of arrays, keyed by error class -> proximate cause |
| ok | number of finding aids successfully ingested | integer |
| bad | number of finding aids that failed to ingest | integer |
| total | number of finding aids processed in total | integer |

## Notes
- repository ids can be found using the api (http://localhost:8089/repositories, for example); they must be parsed out
- really this should handle its own log rotation, sorry, PRs welcome or I'll get to it eventually.
