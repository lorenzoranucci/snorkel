import logging
from xml.dom import minidom

from models import get_sentimantic_session
from snorkel import SnorkelSession
import requests
from snorkel.annotations import LabelAnnotator
from nltk.util import ngrams
import numpy as np
from textacy.similarity  import  levenshtein, jaccard, jaro_winkler, hamming, token_sort_ratio
from snorkel.lf_helpers import (
    get_left_tokens, get_right_tokens, get_between_tokens,
    get_text_between, get_tagged_text,
)

from snorkel.models import  LabelKey


def predicate_candidate_labelling(predicate_resume,  parallelism=8,  test=False, limit=None, replace_key_set=False):
    logging.info("Starting labeling ")
    session = SnorkelSession()
    try:
        candidate_subclass=predicate_resume["candidate_subclass"]
        key_group=predicate_resume["label_group"]
        cids_query=session.query(candidate_subclass.id).filter(candidate_subclass.split == 0)
        if limit !=None:
            cids_query=cids_query.filter(candidate_subclass.id<limit)


        LFs = get_labelling_functions(predicate_resume)

        labeler = LabelAnnotator(lfs=LFs)
        np.random.seed(1701)

        #if first run or adding a new labeling functionS is needed to set replace key set to True
        if not replace_key_set:
            alreadyExistsGroup=session.query(LabelKey).filter(LabelKey.group==key_group).count()>0
            replace_key_set=not alreadyExistsGroup
        L_train = labeler.apply(parallelism=parallelism, cids_query=cids_query,
                                key_group=key_group, clear=False, replace_key_set=replace_key_set)

    finally:
        logging.info("Finished labeling ")





def get_labelling_functions(predicate_resume):
    subject_type=predicate_resume["subject_type"]
    object_type=predicate_resume["object_type"]
    subject_type_split = subject_type.split('/')
    object_type_split = object_type.split('/')
    subject_type_end=subject_type_split[len(subject_type_split)-1]
    object_type_end=object_type_split[len(object_type_split)-1]
    SentimanticSession = get_sentimantic_session()
    sentimantic_session = SentimanticSession()
    sample_class=predicate_resume["sample_class"]
    samples=sentimantic_session.query(sample_class).all()
    known_samples=set()
    for sample in samples:
        known_samples.add((sample.subject,sample.object))

    tmp_words=set([])
    for word in predicate_resume["words"]:
        tmp_words.add(word)
        tmp_words.add(word.title())
    words=tmp_words

    def LF_distant_supervision(c):
        subject_span=getattr(c,"subject").get_span()
        object_span=getattr(c,"object").get_span()
        if (subject_span, object_span)in known_samples:
            return 1

        sample_subject_span= getattr(c,"subject")
        sample_subjects=get_nouns(sample_subject_span,subject_type_end)
        sample_object_span = getattr(c,"object")
        sample_objects=get_nouns(sample_object_span,object_type_end)

        sample_subjects.append(subject_span)
        sample_objects.append(object_span)
        for sample_subject in sample_subjects:
            for sample_object in sample_objects:
                if (sample_subject, sample_object)in known_samples:
                    return 1
        #todo implement date
        #return -1 if len(words.intersection(c.get_parent().words)) < 1 else 0
        return -1 if len(words.intersection(c.get_parent().words)) < 1 else 0
        #return -1 if np.random.rand() < 0.30 else 0
    def LF_distant_supervision_neg(c):
        subject_span=getattr(c,"subject").get_span()
        object_span=getattr(c,"object").get_span()
        if (subject_span, object_span)in known_samples:
            return 0

        sample_subject_span= getattr(c,"subject")
        sample_subjects=get_nouns(sample_subject_span,subject_type_end)
        sample_object_span = getattr(c,"object")
        sample_objects=get_nouns(sample_object_span,object_type_end)

        sample_subjects.append(subject_span)
        sample_objects.append(object_span)
        for sample_subject in sample_subjects:
            for sample_object in sample_objects:
                if (sample_subject, sample_object)in known_samples:
                    return 0
        #todo implement date
        return 1#-1 if len(words.intersection(c.get_parent().words)) < 1 else 0




    def LF_words_between(c):
        if len(words.intersection(get_between_tokens(c))) > 0:
            return 1
        #return -1 if np.random.rand() < 0.10 else 0
        return 0
    def LF_words_left(c):
        if len(words.intersection(get_left_tokens(c))) > 0:
            return 1
        #return -1 if np.random.rand() < 0.10 else 0
        return 0
    def LF_words_right(c):
        if len(words.intersection(get_right_tokens(c))) > 0:
            return 1
        #return -1 if np.random.rand() < 0.10 else 0
        return 0

    Lfs=[
        LF_distant_supervision, LF_words_between, LF_words_left, LF_words_right#, LF_distant_supervision_neg
    ]
    return Lfs


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
        response_ok=False
        while response_ok==False:
            try:
                response_subject=requests.get("http://lookup:1111/api/search/PrefixSearch",{"MaxHits":4,"QueryClass":type,"QueryString":noun})
                response_ok=response_subject.ok
            except Exception:
                print("")

        try:
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
                    # try:
                    #     label=label.decode("utf-8", errors='ignore').replace(",", "").encode("utf-8")
                    # except:
                    #     label=label
                    result.append(label)
        finally:
            if result != None:
                break
    if result == None:
        result=[]
    return result