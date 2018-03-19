import logging
from SPARQLWrapper import SPARQLWrapper, JSON
from models import get_sentimantic_session

def get_predicate_samples_from_KB(predicate_resume, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                                  defaultGraph="http://dbpedia.org"):

    SentimanticSession=get_sentimantic_session()
    sentimantic_session=SentimanticSession()
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)

    logging.info('Starting downloading samples for predicate "%s" domain "%s", range "%s"',
                 predicate_resume["predicate_URI"], predicate_resume["subject_type"], predicate_resume["object_type"])
    sample_class=predicate_resume["sample_class"]

    # build query
    query=get_query(predicate_resume)
    offset = 0
    query_offset = "OFFSET "
    query_limit = "LIMIT 10000"
    results_count = 1
    while results_count > 0 :
        query = query + query_offset + str(offset) + " \n" + query_limit
        print(query)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        results_count = len(results["results"]["bindings"])
        for result in results["results"]["bindings"]:
            try:
                subject = result["subjectLabel"]["value"].encode('utf-8').strip().replace("\"", "")
                object = result["objectLabel"]["value"].encode('utf-8').strip().replace("\"", "")
                already_exist=sentimantic_session.query(sample_class).filter(sample_class.subject==subject, sample_class.object==object).count()>0
                if not already_exist:
                    sentimantic_session.add(sample_class(subject=subject, object=object))
                    sentimantic_session.commit()
            except Exception as e:
                print(e)
        offset += results_count
    logging.info('Finished downloading samples for predicate "%s" domain "%s", range "%s"', predicate_URI, domain, range_)



def get_query(predicate_resume):
    domain=predicate_resume["subject_type"]
    range_=predicate_resume["object_type"]
    object_type_filter=""
    object_variable="objectLabel"
    if (predicate_resume["object_ne"]!="DATE"):
        object_type_filter=""" 
        ?o a <""" + range_ + """>. 
        ?o <http://www.w3.org/2000/01/rdf-schema#label> ?objectLabel .
        FILTER (lang(?objectLabel) = 'en'). 
        """
        object_variable="o"

    query_select = """
    SELECT DISTINCT ?subjectLabel ?objectLabel
    """
    query_where = """
    WHERE{
        ?s <""" + predicate_resume["predicate_URI"] + """> ?"""+object_variable+""" .
        ?s a <""" + domain + """>.
        """+object_type_filter+"""
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
        FILTER (lang(?subjectLabel) = 'en')
    }\n
    """
    query=query_select+query_where
    return query