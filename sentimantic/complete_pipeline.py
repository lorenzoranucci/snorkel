import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@postgres:5432/sentimantic'
import logging
from models import create_database
from wikipedia_client import download_articles
from corpus_parser import parse_wikipedia_dump
from gold_label_creator import create_gold_label
from predicate_utils import save_predicate, infer_and_save_predicate_candidates_types, get_predicate_resume, get_predicate_samples_from_KB
from candidateExtraction import extract_binary_candidates
from labelingFunctionsFactory import predicate_candidate_distant_supervision
from train_model import train_model
from test_model import test_model, before_test

logging.basicConfig(filename='sentimantic.log',level=logging.DEBUG, format='%(asctime)s %(message)s')
dump_file_dir="../../data/wikipedia/dump/en/"
dump_file_name="complete.xml"


def before_start_pipeline(dump_file_path):
    create_database()
    #download some page contents from wikipedia
    titles_list=[
        "Obama",
        "DiCaprio",
        "Del Piero",
        "Ronaldo Cristiano",
        "Elon Musk",
        "Shakira",
        "Francesco Totti",
        "Gianluigi Buffon",
        "Kurt Cobain",
        "Jimmy Page",
        "Robert Plant",
        "Enzo Ferrari",
        "Zinedine Zidane"
    ]
    download_articles(titles_list, dump_file_path)

def start_pipeline(dump_file_dir):
    logging.info("Pipeline start")
    parse_wikipedia_dump(dump_file_dir, clear=clear)
    predicate_URI_list=["http://dbpedia.org/ontology/birthPlace"]
    for predicate_URI in predicate_URI_list:
        start_predicate_pipeline(predicate_URI)

def start_predicate_pipeline(predicate_URI):
    #persist predicate
    save_predicate(predicate_URI)
    #retrieve predicate domain and range
    infer_and_save_predicate_candidates_types(predicate_URI)
    #get predicate with related objects from database
    predicate_resume_list=get_predicate_resume(predicate_URI)
    for predicate_resume in predicate_resume_list:
        start_predicate_domain_range_pipeline(predicate_resume)

def start_predicate_domain_range_pipeline(predicate_resume):
    #download samples from knowledge base
    get_predicate_samples_from_KB(predicate_resume)
    #candidates extraction
    extract_binary_candidates(predicate_resume, clear=clear)

    create_gold_label(predicate_resume)
    #candidates labeling with distant supervision
    predicate_candidate_distant_supervision(predicate_resume, parallel=True, clear=clear, words={"born"}, test=False, limit=100000)
    #todo labeling with predicate specific or domain specific functions
    #train model
    train_model(predicate_resume)
    test_model(predicate_resume)
    #before_test(predicate_resume,"../../data/wikipedia/api/en1/")


clear=True
before_start_pipeline(dump_file_dir+dump_file_name)
start_pipeline(dump_file_dir)