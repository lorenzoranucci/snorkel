import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@postgres:5432/sentimantic'
import sys
from snorkel import SnorkelSession
from snorkel.contrib.brat import BratAnnotator
from predicate_utils import get_predicate_resume
from time import gmtime, strftime


predicate_URI=None
split=0
for arg in sys.argv:
    arg=arg.strip()
    if "http" in arg:
        predicate_URI=arg
    else:
        if arg[0] == 0 or arg[0] == 1 or arg[0] == 2:
            split=arg[0]

def create_collection(predicate_URI, split=None):
    predicate_resume=get_predicate_resume(predicate_URI)[0]
    session = SnorkelSession()
    candidate_subclass = predicate_resume["candidate_subclass"]
    brat = BratAnnotator(session, candidate_subclass, encoding='utf-8')
    predicate_name = predicate_resume["predicate_name"]
    if split==1:
        name="/test"
    elif split==2:
        name="/dev"
    else:
        split=0
        name="/train"

    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())
    brat.init_collection(predicate_name+candidate_subclass.__name__+name+date_time, split=split)

create_collection(predicate_URI,split=split)