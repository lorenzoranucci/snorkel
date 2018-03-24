import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@postgres:5432/sentimantic'
from snorkel import SnorkelSession
from snorkel.contrib.brat import BratAnnotator
from time import gmtime, strftime
import logging



def create_collection(predicate_resume, split):
    session = SnorkelSession()
    CandidateSubclass=predicate_resume["candidate_subclass"]
    if split==1:
        name="/dev"
    elif split==2:
        name="/test"
    else:
        print("No split selected")
        logging.error("No split selected")
    cids_query=session.query(CandidateSubclass.id).filter(CandidateSubclass.split==split)

    candidate_subclass = predicate_resume["candidate_subclass"]
    brat = BratAnnotator(session, candidate_subclass, encoding='utf-8')
    predicate_name = predicate_resume["predicate_name"]
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())
    collection_name=predicate_name+candidate_subclass.__name__+name+date_time
    brat.init_collection(collection_name, cid_query=cids_query)
    return collection_name