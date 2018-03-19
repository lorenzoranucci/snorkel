import logging

import time
from SPARQLWrapper import SPARQLWrapper, JSON

from models import get_sentimantic_session
import threading


def get_predicate_samples_from_KB(predicate_resume, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                                  defaultGraph="http://dbpedia.org", language="en",page_size=10000):


    logging.info('Starting downloading samples for predicate "%s" domain "%s", range "%s"',
                 predicate_resume["predicate_URI"], predicate_resume["subject_type"], predicate_resume["object_type"])

    count=count_samples(predicate_resume, language=language,
                  kb_SPARQL_endpoint=kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    pages=count/page_size+1
    threads = []
    for current_page in range(0,pages):
        offset=current_page*page_size
        try:
            keywords = {'page_size':page_size,'language': language,
                        'kb_SPARQL_endpoint': kb_SPARQL_endpoint,'defaultGraph':defaultGraph}
            t = threading.Thread(target=execute_query, args=(predicate_resume,offset,), kwargs=keywords)
            threads.append(t)
            t.start()
        except Exception:
            logging.error("Fail to create sample download thread")

    while True:
        time.sleep(10)
        for t in threads:
            if  not t.isAlive():
                threads.remove(t)
        if len(threads) < 1:
            break


    logging.info('Finished downloading samples for predicate "%s" domain "%s", range "%s"',
                 predicate_resume["predicate_URI"], predicate_resume["subject_type"], predicate_resume["object_type"])



def execute_query(predicate_resume,offset, page_size=10000, language='en',
                  kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                  defaultGraph="http://dbpedia.org"):
    SentimanticSession=get_sentimantic_session()
    sentimantic_session=SentimanticSession()
    sample_class=predicate_resume["sample_class"]
    query_offset = "OFFSET "
    query_limit = "LIMIT "+str(page_size)
    query=get_query(predicate_resume, language)
    query = query + query_offset + str(offset) + " \n" + query_limit
    print(query)
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    completed=False
    while not completed:
        try:
            results = sparql.query().convert()
            completed=True
            for result in results["results"]["bindings"]:
                try:
                    subject = result["subjectLabel"]["value"].encode('utf-8').strip().replace("\"", "")
                    object = result["objectLabel"]["value"].encode('utf-8').strip().replace("\"", "")
                    #already_exist=sentimantic_session.query(sample_class).filter(sample_class.subject==subject, sample_class.object==object).count()>0
                    #if not already_exist:
                    sentimantic_session.add(sample_class(subject=subject, object=object))
                    sentimantic_session.commit()
                except Exception as e:
                    True
        except Exception as http_error:
            completed=False
            time.sleep(5)



def get_query(predicate_resume, language='en'):
    query_select = """
    SELECT DISTINCT ?subjectLabel ?objectLabel
    """
    return query_select+ get_query_where(predicate_resume, language=language)

def count_samples(predicate_resume,
                  language='en',
                  kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                  defaultGraph="http://dbpedia.org"):
    query=get_count(predicate_resume,language)
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)
    print(query)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    result = results["results"]["bindings"][0]['callret-0']["value"]
    return int(result)

def get_count(predicate_resume, language='en'):
    query_select = """
    SELECT COUNT DISTINCT ?subjectLabel ?objectLabel
    """
    return query_select+ get_query_where(predicate_resume,language=language)


def get_query_where(predicate_resume, language='en'):
    domain=predicate_resume["subject_type"]
    range_=predicate_resume["object_type"]
    object_type_filter=""
    object_variable="objectLabel"
    if (predicate_resume["object_ne"]!="DATE"):
        object_type_filter=""" 
        ?o a <""" + range_ + """>. 
        ?o <http://www.w3.org/2000/01/rdf-schema#label> ?objectLabel .
        FILTER (lang(?objectLabel) = '"""+language+"""'). 
        """
        object_variable="o"


    query_where = """
    WHERE{
        ?s <""" + predicate_resume["predicate_URI"] + """> ?"""+object_variable+""" .
        ?s a <""" + domain + """>.
        """+object_type_filter+"""
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
        FILTER (lang(?subjectLabel) = '"""+language+"""')
    }\n
    """
    return query_where