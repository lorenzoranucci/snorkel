from SPARQLWrapper import SPARQLWrapper, JSON
from type_utils import get_namedentity
from models import get_sentimanctic_session


# def get_predicate_samples_from_KB(predicate_URI, domain, range, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
#                                   defaultGraph="http://dbpedia.org"):
#     sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
#     predicate_split = predicate_URI.split('/')
#     predicate_split_len = len(predicate_split)
#     predicate = predicate_split[predicate_split_len - 1]
#     # create file for output
#     name = predicate + domain + range
#     fn = "./data/" + name + ".csv"
#     file = open(fn, 'a+')
#     import csv
#     writer = csv.writer(file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#
#     # build query
#     query_select = """
#     SELECT DISTINCT ?subjectLabel ?objectLabel \n
#     """
#     query_where = """
#     WHERE{
#         ?s <""" + predicate_URI + """> ?objectLabel.
#         ?s a <""" + domain[1] + """>.
#         ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
#         FILTER (lang(?subjectLabel) = 'en')
#     }\n
#     """
#     offset = 0
#     query_offset = "OFFSET "
#     query_limit = "LIMIT 10000"
#     results_count = 1
#     while results_count > 0 and results_count <= 1000:
#         query = query_select + query_where + query_offset + str(offset) + " \n" + query_limit
#         print(query)
#         sparql.setQuery(query)
#         sparql.setReturnFormat(JSON)
#         results = sparql.query().convert()
#         results_count = len(results["results"]["bindings"])
#         for result in results["results"]["bindings"]:
#             try:
#                 subject = result["subjectLabel"]["value"].encode('utf-8').strip().replace(",", "").replace("\"", "")
#                 object = result["objectLabel"]["value"].encode('utf-8').strip().replace(",", "").replace("\"", "")
#                 writer.writerow([subject, object])
#             except Exception as e:
#                 print(e)
#         offset += results_count
#
#     import bz2
#     from shutil import copyfileobj
#     with bz2.BZ2File(fn + '.bz2', 'wb', compresslevel=9) as output:
#         copyfileobj(file, output)
#
#     import os
#     os.remove(file.name)


def count_predicate_samples(predicate_URI, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                            defaultGraph="http://dbpedia.org"):
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    rangeQuery = """SELECT  COUNT  (?o) AS ?count
        WHERE{
        ?s <""" + predicate_URI + """> ?o.
        }"""
    sparql.setQuery(rangeQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    count_triples = 0
    for result in results["results"]["bindings"]:
        count_triples = int(result["count"]["value"].encode('utf-8').strip())
    return count_triples


def get_predicate_ranges(predicate_URI, kb_SPARQL_endpoint="https://dbpedia.org/sparql", defaultGraph="http://dbpedia.org",
                         samplesCount=1):
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    rangeQuery = """
        SELECT DISTINCT ?range
        WHERE {
            <""" + predicate_URI + """> <http://www.w3.org/2000/01/rdf-schema#range> ?range.
        }"""
    sparql.setQuery(rangeQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    ranges = []
    for result in results["results"]["bindings"]:
        type_URI = result["range"]["value"].encode('utf-8').strip()
        namedEntityType = get_namedentity(type_URI, kb_SPARQL_endpoint)
        if namedEntityType != None:
            range = {'URI': type_URI, 'namedEntityType': namedEntityType}
            ranges.append(range)

    # retrieve types from results
    if (len(ranges) == 0):
        rangeQuery = """
        SELECT ?type COUNT  (?type )AS ?typeCount
        WHERE{
            ?s <http://dbpedia.org/ontology/city> ?o.
            ?o a ?type.
            FILTER( regex(?type, "http://dbpedia.org/ontology/Person", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Place", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Event", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Organisation", "i")
            )
        }
        GROUP BY (?type)
        ORDER BY DESC(?typeCount)"""
        sparql.setQuery(rangeQuery)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            type_URI = result["type"]["value"].encode('utf-8').strip()
            typeCount = int(result["typeCount"]["value"].encode('utf-8').strip())
            if typeCount > samplesCount / 10:
                namedEntityType = get_namedentity(type_URI, kb_SPARQL_endpoint)
                if namedEntityType != None:
                    range = {'URI': type_URI, 'namedEntityType': namedEntityType}
                    ranges.append(range)
    return ranges


def get_predicate_domains(predicate_URI, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                          defaultGraph="http://dbpedia.org", samplesCount=1):
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    domainQuery = """
        SELECT DISTINCT ?domain
        WHERE {
            <""" + predicate_URI + """> <http://www.w3.org/2000/01/rdf-schema#domain> ?domain.
        }"""
    sparql.setQuery(domainQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    domains = []
    for result in results["results"]["bindings"]:
        type_URI = result["domain"]["value"].encode('utf-8').strip()
        namedEntityType = get_namedentity(type_URI, kb_SPARQL_endpoint)
        if namedEntityType != None:
            domain = {'URI': type_URI, 'namedEntityType': namedEntityType}
            domains.append(domain)

    # retrieve types from results
    if (len(domains) == 0):
        domainQuery = """
        SELECT ?type COUNT  (?type )AS ?typeCount
        WHERE{
            ?s <http://dbpedia.org/ontology/city> ?o.
            ?s a ?type.
            FILTER( regex(?type, "http://dbpedia.org/ontology/Person", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Place", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Event", "i") ||
            regex(?type, "http://dbpedia.org/ontology/Organisation", "i")
            )
        }
        GROUP BY (?type)
        ORDER BY DESC(?typeCount)"""
        sparql.setQuery(domainQuery)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            type_URI = result["type"]["value"].encode('utf-8').strip()
            typeCount = int(result["typeCount"]["value"].encode('utf-8').strip())
            if typeCount > samplesCount / 10:
                namedEntityType = get_namedentity(type_URI, kb_SPARQL_endpoint)
                if namedEntityType != None:
                    domain = {'URI': type_URI, 'namedEntityType': namedEntityType}
                    domains.append(domain)
    return domains
