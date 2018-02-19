from SPARQLWrapper import SPARQLWrapper, JSON
from type_utils import get_namedentity
from models import get_sentimanctic_session, Type
from sqlalchemy.exc import IntegrityError
from models import get_sentimanctic_session, Predicate, BinaryCandidate, PredicateCandidateAssoc
from predicate_utils import get_predicate_domains, get_predicate_ranges, count_predicate_samples


def save_predicate(predicate_URI):
    SentimanticSession=get_sentimanctic_session()
    sentimantic_session=SentimanticSession()
    predicate_URI=predicate_URI.strip()
    try:
        new_predicate=Predicate(uri=predicate_URI)
        sentimantic_session.add(new_predicate)
        sentimantic_session.commit()
    except IntegrityError:
        print("Integrity error")
        sentimantic_session.rollback()

def infer_and_save_predicate_candidates_types(predicate_URI):
    SentimanticSession=get_sentimanctic_session()
    sentimantic_session=SentimanticSession()
    predicate_URI=predicate_URI.strip()
    #retrieve predicate domain
    domains=get_predicate_domains(predicate_URI)
    #retrieve predicate range
    ranges=get_predicate_ranges(predicate_URI)

    for domain in domains:
        subject_ne=domain["ne"]
        for range in ranges:
            object_ne=range["ne"]
            candidate=sentimantic_session.query(BinaryCandidate) \
                .filter(BinaryCandidate.subject_namedentity == subject_ne,
                        BinaryCandidate.object_namedentity == object_ne
                        ).first()
            if candidate == None :
                candidate = BinaryCandidate(subject_namedentity=subject_ne,
                                            object_namedentity=object_ne)
                sentimantic_session.add(candidate)
                sentimantic_session.commit()
                candidate=sentimantic_session.query(BinaryCandidate) \
                    .filter(BinaryCandidate.subject_namedentity == subject_ne,
                            BinaryCandidate.object_namedentity == object_ne
                            ).first()


            pca=sentimantic_session.query(PredicateCandidateAssoc) \
                .filter(PredicateCandidateAssoc.predicate_id == predicate_URI,
                        PredicateCandidateAssoc.candidate_id == candidate.id
                        ).first()
            if pca == None:
                pca = PredicateCandidateAssoc(predicate_id=predicate_URI,
                                              candidate_id=candidate.id)
                sentimantic_session.add(pca)
                sentimantic_session.commit()
                pca=sentimantic_session.query(PredicateCandidateAssoc) \
                    .filter(PredicateCandidateAssoc.predicate_id == predicate_URI,
                            PredicateCandidateAssoc.candidate_id == candidate.id
                            ).first()

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

def get_predicate_ranges(predicate_URI, kb_SPARQL_endpoint="https://dbpedia.org/sparql", defaultGraph="http://dbpedia.org"):
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
        ne = get_namedentity(type_URI, kb_SPARQL_endpoint, defaultGraph=defaultGraph)
        if ne != None:
            range = {'URI': type_URI, 'ne': ne}
            ranges.append(range)

    # retrieve types from results
    if (len(ranges) == 0):
        rangeQueryTotal="""
        SELECT ?type COUNT  (?type) AS ?typeCount
        WHERE{
        {   SELECT ?type
            WHERE{
            ?s <"""+predicate_URI+"""> ?o.
            ?o a ?type.
            }
            LIMIT 100000
        }
           FILTER(  regex(?type, "http://www.w3.org/2002/07/owl#Thing", "i")  )
        }
        GROUP BY (?type)
        ORDER BY DESC(?typeCount)"""
        sparql.setQuery(rangeQueryTotal)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        totalCount=0
        for result in results["results"]["bindings"]:
            totalCount = int(result["typeCount"]["value"].encode('utf-8').strip())
            break

        if totalCount>0:
            rangeQuery = """
            SELECT ?type COUNT  (?type) AS ?typeCount
            WHERE{
            {   SELECT ?type
                WHERE{
                ?s <"""+predicate_URI+"""> ?o.
                ?o a ?type.
                }
                LIMIT 100000
            }
                """+get_types_filter_regex()+"""
            }
            GROUP BY (?type)
            ORDER BY DESC(?typeCount)"""
            sparql.setQuery(rangeQuery)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            for result in results["results"]["bindings"]:
                type_URI = result["type"]["value"].encode('utf-8').strip()
                typeCount = int(result["typeCount"]["value"].encode('utf-8').strip())
                if typeCount > (totalCount / 100)*3:
                    ne = get_namedentity(type_URI, kb_SPARQL_endpoint, defaultGraph=defaultGraph)
                    if ne != None:
                        range = {'URI': type_URI, 'ne': ne}
                        ranges.append(range)
    return ranges


def get_predicate_domains(predicate_URI, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                          defaultGraph="http://dbpedia.org"):
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
        ne = get_namedentity(type_URI, kb_SPARQL_endpoint, defaultGraph=defaultGraph)
        if ne != None:
            domain = {'URI': type_URI, 'ne': ne}
            domains.append(domain)

    # retrieve types from results
    if (len(domains) == 0):
        domainQueryTotal="""
        SELECT ?type COUNT  (?type) AS ?typeCount
        WHERE{
        {   SELECT ?type
            WHERE{
            ?s <"""+predicate_URI+"""> ?o.
            ?s a ?type.
            }
            LIMIT 100000
        }
           FILTER(  regex(?type, "http://www.w3.org/2002/07/owl#Thing", "i")  )
        }
        GROUP BY (?type)
        ORDER BY DESC(?typeCount)"""
        sparql.setQuery(domainQueryTotal)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        totalCount=0
        for result in results["results"]["bindings"]:
            totalCount = int(result["typeCount"]["value"].encode('utf-8').strip())
            break

        if totalCount>0:
            domainQuery = """
            SELECT ?type COUNT  (?type) AS ?typeCount
            WHERE{
            {   SELECT ?type
                WHERE{
                ?s <"""+predicate_URI+"""> ?o.
                ?s a ?type.
                }
                LIMIT 100000
            }
                """+get_types_filter_regex()+"""
            }
            GROUP BY (?type)
            ORDER BY DESC(?typeCount)"""
            sparql.setQuery(domainQuery)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            for result in results["results"]["bindings"]:
                type_URI = result["type"]["value"].encode('utf-8').strip()
                typeCount = int(result["typeCount"]["value"].encode('utf-8').strip())
                if typeCount > (totalCount / 100)*3:
                    ne = get_namedentity(type_URI, kb_SPARQL_endpoint, defaultGraph=defaultGraph)
                    if ne != None:
                        domain = {'URI': type_URI, 'ne': ne}
                        domains.append(domain)
    return domains

def get_types_filter_regex():
    SentimanticSession=get_sentimanctic_session()
    sentimantic_session=SentimanticSession()
    types=sentimantic_session.query(Type).all()
    i=0
    filter="FILTER( "
    for type in types:
        if i!=0:
           filter=filter+" || "
        filter=filter+""" regex(?type, \""""+type.uri+"""\", "i") """
        i=i+1
    filter=filter+" ) "

    return filter

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