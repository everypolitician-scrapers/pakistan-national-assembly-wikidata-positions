#!/bin/env ruby
# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'pry'
require 'scraperwiki'
require 'wikidata/fetcher'

WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql'

def sparql(query)
  result = RestClient.get WIKIDATA_SPARQL_URL, accept: 'text/csv', params: { query: query }
  CSV.parse(result, headers: true, header_converters: :symbol)
rescue RestClient::Exception => e
  raise "Wikidata query #{query} failed: #{e.message}"
end

def wikidata_id(url)
  url.to_s.split('/').last
end

memberships_query = <<EOQ
SELECT DISTINCT ?item ?itemLabel ?start_date ?end_date ?hasRoleLabel ?constituency ?constituencyLabel ?party ?partyLabel ?partyShortname ?term ?termLabel ?termOrdinal ?scraperName WHERE {
  ?item p:P39 ?statement.
  ?statement ps:P39 wd:Q33512801; pq:P2937 wd:Q29068722 .
  OPTIONAL { ?statement pq:P580 ?start_date. }
  OPTIONAL { ?statement pq:P582 ?end_date. }
  OPTIONAL { ?statement pq:P768 ?constituency. }
  OPTIONAL {
    ?statement pq:P4100 ?party .
    OPTIONAL { ?party wdt:P1813 ?partyShortname . }
  }
  OPTIONAL { ?statement pq:P2868 ?hasRole. }
  OPTIONAL {
    ?statement pq:P2937 ?term .
    OPTIONAL { ?term p:P31/pq:P1545 ?termOrdinal . }
  }
  OPTIONAL {
    ?item p:P973 ?described .
    ?described ps:P973 ?url .
    ?described pq:P1810 ?scraperName .
    FILTER(CONTAINS(LCASE(STR(?url)), "www.na.gov.pk/"))
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
  FILTER(!BOUND(?end_date) || ?end_date > NOW())
  FILTER (LANG(?partyShortname) = 'en')
}
EOQ

data = sparql(memberships_query).map(&:to_h).map do |r|
  {
    id:                          wikidata_id(r[:item]),
    name:                        r[:scrapername].to_s.empty? ? r[:itemlabel] : r[:scrapername],
    start_date:                  r[:start_date].to_s[0..9],
    end_date:                    r[:end_date].to_s[0..9],
    legislative_membership_type: r[:hasrolelabel].to_s.empty? ? '' : r[:hasrolelabel],
    constituency:                r[:constituencylabel],
    constituency_id:             r[:constituencylabel][/(NA-[0-9]+)/, 1] || wikidata_id(r[:constituency]),
    party:                       r[:partyshortname].to_s.empty? ? r[:partylabel] : r[:partyshortname],
    party_id:                    r[:partyshortname].to_s.empty? ? wikidata_id(r[:party]) : r[:partyshortname].downcase.tr('^a-z', ''),
    term:                        r[:termordinal],
  }
end

ScraperWiki.sqliteexecute('DROP TABLE data') rescue nil
ScraperWiki.save_sqlite(%i[id], data)
