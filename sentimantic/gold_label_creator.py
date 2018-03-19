import subprocess
from snorkel import SnorkelSession
from snorkel.models import  Document
from snorkel.contrib.brat import BratAnnotator

def create_gold_label(predicate_resume):
    session = SnorkelSession()
    candidate_subclass = predicate_resume["candidate_subclass"]
    brat = BratAnnotator(session, candidate_subclass, encoding='utf-8')
    predicate_URI=predicate_resume["predicate_URI"]
    predicate_name = predicate_resume["predicate_name"]
    brat.init_collection(predicate_name+candidate_subclass.__name__+"/train", split=0)

    proc = subprocess.Popen("./run_brat.sh", shell=True,
                            stdin=None, stdout=None, stderr=None, close_fds=True)
    return_code=proc.poll()
    while return_code == None:
        print("DBpedia Lookup lanching...")
        return_code=proc.poll()