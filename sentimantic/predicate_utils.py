import logging
from SPARQLWrapper import SPARQLWrapper, JSON

from type_utils import get_namedentity
from models import  Type, TypeNamedEntityAssoc
from sqlalchemy.exc import IntegrityError
from models import get_sentimantic_engine,get_sentimantic_session, Predicate, BinaryCandidate, PredicateCandidateAssoc, get_predicate_candidate_samples_table
from snorkel.models import candidate_subclass
from sqlalchemy.sql import text


def save_predicate(predicate_URI):
    logging.info('Saving predicate "%s"', predicate_URI)
    SentimanticSession=get_sentimantic_session()
    sentimantic_session=SentimanticSession()
    predicate_URI=predicate_URI.strip()
    try:
        new_predicate=Predicate(uri=predicate_URI)
        sentimantic_session.add(new_predicate)
        sentimantic_session.commit()
    except IntegrityError:
        logging.warn('Predicate "%s" already existing', predicate_URI )
        sentimantic_session.rollback()
    logging.info('Predicate "%s" saved', predicate_URI)


def get_predicate_resume(predicate_URI):

    result=[]
    SentimanticSession = get_sentimantic_session()
    sentimantic_session = SentimanticSession()
    predicate=sentimantic_session.query(Predicate).filter(Predicate.uri==predicate_URI).first()
    if predicate != None:
        predicate_URI=predicate_URI.strip()
        pca_list=sentimantic_session.query(PredicateCandidateAssoc) \
            .filter(PredicateCandidateAssoc.predicate_id == predicate.id).all()
        for pca in pca_list:
            candidate=sentimantic_session.query(BinaryCandidate) \
                .filter(BinaryCandidate.id==pca.candidate_id).first()
            subject_ne=candidate.subject_namedentity.strip()
            object_ne=candidate.object_namedentity.strip()
            candidate_name=(subject_ne+object_ne).encode("utf-8")
            CandidateSubclass = candidate_subclass(candidate_name,
                                                   ["subject_"+subject_ne.lower(),
                                                    "object_"+object_ne.lower()
                                                    ])
            try:
                statement = text("""
        CREATE OR REPLACE VIEW """+ candidate_name.lower() +"""_view AS
            SELECT document.id AS docid,
        document.name AS docname,
        """+ candidate_name.lower() +""".id AS candid,
        candidate.split,
        sentence.text,
        predicate.uri as predicate_URI,
        label.value AS label_value,
        marginal.probability
       FROM """+ candidate_name.lower() +"""
         JOIN candidate ON candidate.id = """+ candidate_name.lower() +""".id
         JOIN span ON """+ candidate_name.lower() +""".subject_person_id = span.id
         JOIN sentence ON span.sentence_id = sentence.id
         JOIN document ON sentence.document_id = document.id
         LEFT JOIN label ON candidate.id = label.candidate_id
         LEFT JOIN label_key ON label.key_id = label_key.id
         LEFT JOIN predicate_candidate_assoc ON label_key."group" = predicate_candidate_assoc.id
         left join predicate on predicate_candidate_assoc.predicate_id=predicate.id 
         LEFT JOIN marginal ON marginal.candidate_id = candidate.id;
         
         """)
                get_sentimantic_engine().execute(statement)
            except Exception:
                print("Skip view creation")
            subject_type=sentimantic_session.query(TypeNamedEntityAssoc) \
                .filter(TypeNamedEntityAssoc.namedentity == subject_ne).first().type
            object_type=sentimantic_session.query(TypeNamedEntityAssoc) \
                .filter(TypeNamedEntityAssoc.namedentity == object_ne).first().type

            predicate_split = predicate_URI.split('/')
            predicate_split_len = len(predicate_split)
            predicate_name = predicate_split[predicate_split_len - 1].strip()

            sample_class=get_predicate_candidate_samples_table("Sample"+predicate_name.title()+subject_ne.title()+object_ne.title())
            result.append({"predicate_name": predicate_name,
                            "predicate_URI": predicate_URI,
                            "candidate_subclass": CandidateSubclass,
                            "subject_ne":subject_ne, "object_ne":object_ne,
                            "subject_type":subject_type, "object_type":object_type,
                            "label_group":pca.id, "sample_class": sample_class
                           })
    return result



def infer_and_save_predicate_candidates_types(predicate_URI, sample_files_base_path="./data/samples/"):
    logging.info('Starting infering predicate "%s" domain, range and candidates types ', predicate_URI)
    SentimanticSession=get_sentimantic_session()
    sentimantic_session=SentimanticSession()
    predicate_URI=predicate_URI.strip()
    #retrieve predicate domain
    domains=get_predicate_domains(predicate_URI)
    #retrieve predicate range
    ranges=get_predicate_ranges(predicate_URI)
    predicate=sentimantic_session.query(Predicate).filter(Predicate.uri==predicate_URI).first()
    if predicate != None:
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
                    .filter(PredicateCandidateAssoc.predicate_id == predicate.id,
                            PredicateCandidateAssoc.candidate_id == candidate.id
                            ).first()
                if pca == None:
                    predicate_split = predicate_URI.split('/')
                    predicate_split_len = len(predicate_split)
                    predicate_name = predicate_split[predicate_split_len - 1].strip()
                    pca = PredicateCandidateAssoc(predicate_id=predicate.id,
                                                  candidate_id=candidate.id)
                    sentimantic_session.add(pca)
                    sentimantic_session.commit()
    logging.info('Finished infering predicate "%s" domain, range and candidates types ', predicate_URI)

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
    SentimanticSession=get_sentimantic_session()
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

def get_predicate_samples_from_KB(predicate_resume, kb_SPARQL_endpoint="https://dbpedia.org/sparql",
                                  defaultGraph="http://dbpedia.org"):

    SentimanticSession=get_sentimantic_session()
    sentimantic_session=SentimanticSession()
    sparql = SPARQLWrapper(kb_SPARQL_endpoint, defaultGraph=defaultGraph)

    predicate_URI=predicate_resume["predicate_URI"]
    domain=predicate_resume["subject_type"]
    range=predicate_resume["object_type"]
    logging.info('Starting downloading samples for predicate "%s" domain "%s", range "%s"', predicate_URI, domain, range)
    sample_class=predicate_resume["sample_class"]

    # build query
    object_type_filter=""
    object_variable="objectLabel"
    if (predicate_resume["object_ne"]!="DATE"):
        object_type_filter=""" 
        ?o a <""" + range + """>. 
        ?o <http://www.w3.org/2000/01/rdf-schema#label> ?objectLabel .
        FILTER (lang(?objectLabel) = 'en'). 
        """
        object_variable="o"

    query_select = """
    SELECT DISTINCT ?subjectLabel ?objectLabel
    """
    query_where = """
    WHERE{
        ?s <""" + predicate_URI + """> ?"""+object_variable+""" .
        ?s a <""" + domain + """>.
        """+object_type_filter+"""
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
        FILTER (lang(?subjectLabel) = 'en')
    }\n
    """
    offset = 0
    query_offset = "OFFSET "
    query_limit = "LIMIT 10000"
    results_count = 1
    while results_count > 0 :
        query = query_select + query_where + query_offset + str(offset) + " \n" + query_limit
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
    logging.info('Finished downloading samples for predicate "%s" domain "%s", range "%s"', predicate_URI, domain, range)



def get_predicates_from_config(path="./predicates_list.config"):
    content=[]
    with open(path) as f:
        content = f.readlines()
        # you may also want to remove whitespace characters like `\n` at the end of each line
        content = [x.strip('\n') for x in content]
    return content


