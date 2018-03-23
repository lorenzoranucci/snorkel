import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@postgres:5432/sentimantic'
import sys
from snorkel import SnorkelSession
from snorkel.contrib.brat import BratAnnotator
from predicate_utils import get_predicate_resume
from time import gmtime, strftime
import logging


predicate_URI=None
split=0
for arg in sys.argv:
    arg=arg.strip()
    if "http" in arg:
        predicate_URI=arg
    else:
        if arg[0] == 0 or arg[0] == 1 or arg[0] == 2:
            split=arg[0]

def create_collection(predicate_resume, split):
    if split==1:
        name="/dev"
    elif split==2:
        name="/test"
    else:
        print("No split selected")
        logging.error("No split selected")
    session = SnorkelSession()
    candidate_subclass = predicate_resume["candidate_subclass"]
    brat = BratAnnotator(session, candidate_subclass, encoding='utf-8')
    predicate_name = predicate_resume["predicate_name"]
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())
    collection_name=predicate_name+candidate_subclass.__name__+name+date_time
    brat.init_collection(collection_name, split=split)
    return collection_name