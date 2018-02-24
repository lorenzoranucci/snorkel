import csv
from xml.dom import minidom
from snorkel import SnorkelSession
from predicate_utils import get_predicate_candidates_and_samples_file
import requests
from snorkel.annotations import LabelAnnotator
import nltk
from nltk import word_tokenize
from nltk.util import ngrams
from collections import Counter
from itertools import chain


def predicate_candidate_distant_supervision(predicate_resume):
        samples_file_path=predicate_resume["samples_file_path"]
        candidate_subclass=predicate_resume["candidate_subclass"]
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
            sample_subject_span= getattr(c,"subject_"+predicate_resume["subject_ne"].lower())
            sample_subject=get_noun(sample_subject_span,subject_type_end)
            sample_object_span = getattr(c,"object_"+predicate_resume["object_ne"].lower())
            sample_object=get_noun(sample_object_span,object_type_end)

            #from dateutil.parser import parse
            #sampleObject = parse(sampleObject)
            return 1 if (sample_subject, sample_object) in known_samples else 0

        session = SnorkelSession()
        labeled = []
        i=0
        for c  in session.query(candidate_subclass).filter(candidate_subclass.split == 1).all():
            i=i+1
            if LF_distant_supervision(c) != 0:
                labeled.append(c)
        print("Number labeled:", len(labeled))


        LFs = [
            LF_distant_supervision
        ]

        labeler = LabelAnnotator(lfs=LFs)



def get_noun(span, type):
    ngrams=get_clean_noun_ngrams(span)
    if len(ngrams)<1:
        return span.get_span()
    return get_dbpedia_noun(ngrams,type)


def get_clean_noun_ngrams(span):
    start_word=span.get_word_start()
    end_word=span.get_word_end()
    sentence_pos_tags=span.sentence.pos_tags
    sentence_words=span.sentence.words
    result_tmp=[]
    for i in range(start_word,end_word+1):
        if 'NN' in sentence_pos_tags[i]:
            result_tmp.append(sentence_words[i])

    result=[]
    for i in range(len(result_tmp),0,-1):
        for j in ngrams(result_tmp,i):
            result.append(j)
    return result

def get_dbpedia_noun(ngrams, type):
    result=None
    for ngram in ngrams:
        noun=' '.join(ngram)
        response_subject=requests.get("http://localhost:1111/api/search/PrefixSearch",{"MaxHits":1,"QueryClass":type,"QueryString":noun})
        if(response_subject.ok):
            xml=response_subject.content
            xml=minidom.parseString(xml)
            for element in xml.getElementsByTagName('Label'):
                result=element.firstChild.nodeValue
                break
        if result != None:
            break
    if result == None:
        result= ' '.join(ngrams[0])
    try:
        return result.decode("utf-8", errors='ignore').replace(",", "").encode("utf-8")
    except:
        return result