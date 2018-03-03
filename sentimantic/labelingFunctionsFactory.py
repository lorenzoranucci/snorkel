import os
import csv
import logging
import subprocess
from xml.dom import minidom

from snorkel import SnorkelSession
import requests
from snorkel.annotations import LabelAnnotator
from nltk.util import ngrams
import numpy as np
from snorkel.learning import GenerativeModel
from snorkel.annotations import save_marginals
from textacy.similarity  import word2vec, levenshtein, jaccard, jaro_winkler, hamming, token_sort_ratio

from snorkel.models import Label


def predicate_candidate_distant_supervision(predicate_resume, lfs=[], parallel=True, clear=False):
    logging.info("Starting labeling with distant supervision ")
    session = SnorkelSession()
    # if clear == False:
    #     lab_count = session.query(Label).count()
    #     if lab_count > 1:
    #         logging.warn("Labelling already done, skipping...")
    #         return
    #run dbpedia lookup for distant supervision
    proc = subprocess.Popen("./run_lookup.sh", shell=True,
                            stdin=None, stdout=None, stderr=None, close_fds=True)
    return_code=proc.poll()
    while return_code == None:
        print("DBpedia Lookup lanching...")
        return_code=proc.poll()
    try:

        samples_file_path=predicate_resume["samples_file_path"]
        subject_type=predicate_resume["subject_type"]
        object_type=predicate_resume["object_type"]
        subject_type_split = subject_type.split('/')
        object_type_split = object_type.split('/')
        subject_type_end=subject_type_split[len(subject_type_split)-1]
        object_type_end=object_type_split[len(object_type_split)-1]


        with open(samples_file_path, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            known_samples = set(
                tuple(row) for row in reader
            )

        def LF_distant_supervision(c):
            subject_span=getattr(c,"subject_"+predicate_resume["subject_ne"].lower()).get_span()
            object_span=getattr(c,"object_"+predicate_resume["object_ne"].lower()).get_span()
            if subject_span=='Conegliano' or object_span=="Conegliano":
                print("eccolo")
            if (subject_span, object_span)in known_samples:
                return 1

            sample_subject_span= getattr(c,"subject_"+predicate_resume["subject_ne"].lower())
            sample_subjects=get_nouns(sample_subject_span,subject_type_end)
            sample_object_span = getattr(c,"object_"+predicate_resume["object_ne"].lower())
            sample_objects=get_nouns(sample_object_span,object_type_end)

            sample_subjects.append(subject_span)
            sample_objects.append(object_span)
            for sample_subject in sample_subjects:
                for sample_object in sample_objects:
                    if (sample_subject, sample_object)in known_samples:
                        return 1
            #from dateutil.parser import parse
            #sampleObject = parse(sampleObject)
            return -1


        # labeled = []
        # i=0
        # for c  in session.query(candidate_subclass).filter(candidate_subclass.split == 1).all():
        #     i=i+1
        #     if LF_distant_supervision(c) != 0:
        #         labeled.append(c)
        # print("Number labeled:", len(labeled))


        LFs = [
            LF_distant_supervision,

        ]

        LFs.extend(lfs)
        labeler = LabelAnnotator(lfs=LFs)
        np.random.seed(1701)
        parallelism=None
        if parallel == True and 'SNORKELDB' in os.environ and os.environ['SNORKELDB'] != '' :
            parallelism=20

        L_train = labeler.apply(split=0, parallelism=parallelism)
        #L_train = labeler.load_matrix(session, split=0)
        L_train


        # print(L_train.get_candidate(session, 0))
        # print(L_train.get_key(session, 0))
        # print(L_train.lf_stats(session))
        gen_model = GenerativeModel()
        gen_model.train(L_train, epochs=100, decay=0.95, step_size=0.1 / L_train.shape[0], reg_param=1e-6)
        # gen_model.weights.lf_accuracy
        train_marginals = gen_model.marginals(L_train)
        # plt.hist(train_marginals, bins=20)
        # plt.show()
        # gen_model.learned_lf_stats()

        #label dev
        # L_dev = labeler.apply_existing(split=2, parallelism=parallelism)
        # L_dev
        # print(L_dev.lf_stats(session))

        save_marginals(session, L_train, train_marginals)
        logging.info("Finished labeling with distant supervision ")
    finally:
        proc = subprocess.Popen("./stop_lookup.sh", shell=True,
                                stdin=None, stdout=None, stderr=None, close_fds=True)
        return_code=proc.poll()
        while return_code == None:
            print("DBpedia Lookup stopping...")
            return_code=proc.poll()



def get_nouns(span, type):
    clean_noun=get_clean_noun(span)
    ngrams=get_ngrams(clean_noun)
    if len(ngrams)<1:
        return span.get_span()
    return get_dbpedia_noun(ngrams,type)


def get_clean_noun(span):
    start_word=span.get_word_start()
    end_word=span.get_word_end()
    sentence_pos_tags=span.sentence.pos_tags
    sentence_words=span.sentence.words
    result=[]
    for i in range(start_word,end_word+1):
        if 'NN' in sentence_pos_tags[i]:
            result.append(sentence_words[i])
    return ' '.join(result)

def get_ngrams(string_):
    string_list=string_.split(" ")
    result=[]
    for i in range(len(string_list),0,-1):
        for j in ngrams(string_list,i):
            result.append(j)
    return result

def get_dbpedia_noun(ngrams, type):
    result=None
    for ngram in ngrams:
        noun=' '.join(ngram)
        response_subject=requests.get("http://localhost:1111/api/search/PrefixSearch",{"MaxHits":4,"QueryClass":type,"QueryString":noun})
        try:
            if(response_subject.ok):
                max_refcount=-1
                xml=response_subject.content
                xml=minidom.parseString(xml)
                result_elements=xml.getElementsByTagName('Result')
                for result_element in result_elements:
                    #check refcount
                    current_refcount=int(result_element.getElementsByTagName('Refcount')[0].firstChild.nodeValue)
                    if max_refcount==-1:
                        max_refcount= current_refcount
                    if current_refcount < (max_refcount/10):
                        break
                    label =result_element.getElementsByTagName('Label')[0].firstChild.nodeValue
                    # jaccard, jaro_winkler, hamming, token_sort_ratio
                    jaccardD=jaccard(label,noun)
                    jaro=jaro_winkler(label,noun)
                    lev=levenshtein(label,noun)
                    hammingD=hamming(label,noun)
                    tsr=token_sort_ratio(label,noun)

                    if lev > 0.36:
                        if result==None:
                            result=[]
                        try:
                            label=label.decode("utf-8", errors='ignore').replace(",", "").encode("utf-8")
                        except:
                            label=label
                        result.append(label)
        finally:
            if result != None:
                break
    if result == None:
        result=[]
    return result
