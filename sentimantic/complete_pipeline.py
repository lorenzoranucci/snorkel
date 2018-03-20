from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import os
os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@postgres:5432/sentimantic'
import logging
from models import create_database
from predicate_utils import save_predicate, get_predicate_resume, get_predicates_from_config
from corpus_parser import parse_wikipedia_dump
from infer_predicate_types import infer_and_save_predicate_candidates_types
from download_predicate_candidates_samples import get_predicate_samples_from_KB
from candidate_extraction import extract_binary_candidates
from labelling import predicate_candidate_labelling
from train_model import train_model
from test_model import test_model

logging.basicConfig(filename='sentimantic.log',level=logging.INFO, format='%(asctime)s %(message)s')
dump_file_dir="../../data/wikipedia/dump/en/extracted_text/AA/"
dump_file_name="wiki_00.xml"
parallelism=32
is_to_parse_wikipedia_dump=False
is_to_infer_candidate_types=False
is_to_download_samples_from_kb=False
is_to_extract_candidates=False
is_to_label=False
is_to_train_classifier=False
is_to_test_classifier=False
i=0
for arg in sys.argv:
    if arg.strip()=='parse':
        is_to_parse_wikipedia_dump=True
    elif arg.strip()=='infer':
        is_to_infer_candidate_types=True
    elif arg.strip()=='download':
        is_to_download_samples_from_kb=True
    elif arg.strip()=='extract':
        is_to_extract_candidates=True
    elif arg.strip()=='label':
        is_to_label=True
    elif arg.strip()=='train':
        is_to_train_classifier=True
    elif arg.strip()=='test':
        is_to_test_classifier=True
    elif arg.strip()=='parallelism':
        parallelism=sys.argv[i+1]
    i=i+1



def start_pipeline():
    create_database()
    logging.info("Pipeline start")
    if is_to_parse_wikipedia_dump:
        parse_wikipedia_dump(dump_file_dir, parallelism=parallelism)
    predicate_URI_list=get_predicates_from_config()
    for predicate_URI in predicate_URI_list:
        start_predicate_pipeline(predicate_URI)

def start_predicate_pipeline(predicate_URI):
    #persist predicate
    save_predicate(predicate_URI)
    #retrieve predicate domain and range
    if is_to_infer_candidate_types:
        infer_and_save_predicate_candidates_types(predicate_URI)
    #get predicate with related objects from database
    predicate_resume_list=get_predicate_resume(predicate_URI)
    for predicate_resume in predicate_resume_list:
        start_predicate_domain_range_pipeline(predicate_resume)

def start_predicate_domain_range_pipeline(predicate_resume):
    #download samples from knowledge base
    if is_to_download_samples_from_kb:
        get_predicate_samples_from_KB(predicate_resume, parallelism=parallelism)
    #candidates extraction
    if is_to_extract_candidates:
        extract_binary_candidates(predicate_resume, parallelism=parallelism)
    #candidates labeling with distant supervision
    if is_to_label:
        predicate_candidate_labelling(predicate_resume, parallelism=parallelism,   test=False)
    #train model
    if is_to_train_classifier:
        train_model(predicate_resume, parallelism=parallelism)
    if is_to_test_classifier:
        test_model(predicate_resume, model_name="DbirthPlace18-03-2018_19_54_43")




start_pipeline()