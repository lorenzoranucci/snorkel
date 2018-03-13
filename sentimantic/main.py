# TO USE A DATABASE OTHER THAN SQLITE, USE THIS LINE
# Note that this is necessary for parallel execution amongst other things...
import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@localhost:5432/snorkel'
from train_model import train_model
from labelingFunctionsFactory import predicate_candidate_distant_supervision
from sqlalchemy.exc import IntegrityError
from models import get_sentimanctic_session, Predicate, BinaryCandidate, PredicateCandidateAssoc
from predicate_utils import infer_and_save_predicate_candidates_types, save_predicate, get_predicate_resume, get_predicate_samples_from_KB
from candidateExtraction import  extract_binary_candidates
from corpus_parser import parse_wikipedia_dump
import logging

logging.basicConfig(filename='sentimantic.log',level=logging.DEBUG, format='%(asctime)s %(message)s')

logging.info("Starting...")



#get_predicate_candidates_and_samples_file("http://dbpedia.org/ontology/country")

#parse_wikipedia_dump()


#predicates_list = [
#                    "http://dbpedia.org/ontology/country",
#                    "http://dbpedia.org/ontology/birthDate",
#                    "http://dbpedia.org/ontology/location",
#                    "http://dbpedia.org/ontology/isPartOf",
#                    "http://dbpedia.org/ontology/team",
#                    "http://dbpedia.org/ontology/deathPlace",
#                    "http://dbpedia.org/ontology/almaMater",
#                    "http://dbpedia.org/ontology/league",
#                    "http://dbpedia.org/ontology/city",
#                    "http://dbpedia.org/ontology/starring",
#                    "http://dbpedia.org/ontology/party",
#                    "http://dbpedia.org/ontology/parent",
#                    "http://dbpedia.org/ontology/spouse",
#                    "http://dbpedia.org/ontology/award",
#                    "http://dbpedia.org/ontology/network",
#                    "http://dbpedia.org/ontology/hometown",
#                    "http://dbpedia.org/ontology/formerTeam",
#                    "http://dbpedia.org/ontology/recordLabel"
#                   ]
#
#for predicate_URI in predicates_list:
#    save_predicate(predicate_URI)
#    infer_and_save_predicate_candidates_types(predicate_URI)
#
#
#

#SentimanticSession=get_sentimanctic_session()
#sentimantic_session=SentimanticSession()
#candidates=sentimantic_session.query(BinaryCandidate).first()
#extract_binary_candidates([candidates])

#get_predicate_samples_from_KB("http://dbpedia.org/ontology/country", "http://dbpedia.org/ontology/Place", "http://dbpedia.org/ontology/Place")
#predicate_candidate_distant_supervision(get_predicate_resume("http://dbpedia.org/ontology/country")[0])
train_model(get_predicate_resume("http://dbpedia.org/ontology/country")[0])