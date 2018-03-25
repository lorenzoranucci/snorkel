import logging
from snorkel import SnorkelSession
from snorkel.candidates import Ngrams, CandidateExtractor
from snorkel.matchers import PersonMatcher, DateMatcher,  OrganizationMatcher
from matchers import GPEMatcher, EventMatcher, WorkOfArtMatcher, LanguageMatcher
from models import *


def extract_binary_candidates(predicate_resume, clear=False, parallelism=8,
                              split=None, documents_titles=None, limit=None,
                              page_size=10000):
    #create span and candidates
    logging.info("Starting candidates extraction ")
    subject_ne=predicate_resume['subject_ne']
    object_ne=predicate_resume['object_ne']

    session = SnorkelSession()
    CandidateSubclass = predicate_resume["candidate_subclass"]


    ngrams= Ngrams(n_max=10)
    subject_matcher = get_matcher(subject_ne)
    object_matcher = get_matcher(object_ne)
    cand_extractor = CandidateExtractor(CandidateSubclass,
                                        [ngrams, ngrams],
                                        [subject_matcher,object_matcher],
                                        self_relations=True,
                                        nested_relations=True,
                                        symmetric_relations=True)

    #skip sentences already extracted
    sents_query_id = session.query(Sentence.id)
    candidates_count = session.query(CandidateSubclass).count()
    delete_orphan_spans()
    if documents_titles==None and candidates_count>1 and clear==False:
        sents_query_id = get_sentences_ids_not_extracted(predicate_resume, session)
    elif documents_titles != None:
        #delete candidates for test and dev
        logging.info("Deleting candidates")
        print("Deleting candidates")
        candidates_to_delete=get_cands_to_delete_by_title(predicate_resume,session,documents_titles).all()
        for candidate_to_delete in candidates_to_delete:
            session.delete(candidate_to_delete)
        session.commit()
        sents_query_id=get_sentences_ids_by_title_with_span(predicate_resume,session,documents_titles)

    if limit is not None:
        sents_query_id=sents_query_id.limit(limit)


    sents_query=session.query(Sentence).filter(Sentence.id.in_(sents_query_id))


    sents_count=sents_query.count()

    if sents_count > page_size:
        page=page_size
    else:
        page=sents_count
    i=1
    while(True):
        set_name=""
        if split == None:
            set_name="train"
            split2=0
        else:
            set_name=str(split)
            split2=split

        logging.debug('\tQuering sentences from %s to %s, in set \'%s\'', (page*(i-1)), page*i, set_name)
        sents=sents_query.order_by(Sentence.id).slice((page*(i-1)), page*i).all()
        if sents == None or len(sents) < 1 :
            break
        cand_extractor.apply(sents, split=split2, clear=clear, progress_bar=False, parallelism=parallelism)
        extracted_count=session.query(CandidateSubclass).filter(CandidateSubclass.split == split).count()
        logging.debug('\t\t%d candidates extracted for %s', extracted_count, CandidateSubclass.__name__)
        i=i+1
        clear=False
    logging.info("Finished candidates extraction ")


def get_matcher(ne):
    if ne=="PERSON":
        return PersonMatcher(longest_match_only=True)
    elif ne == "DATE":
        return DateMatcher(longest_match_only=True)
    elif ne == "GPE":
        return GPEMatcher(longest_match_only=True)
    elif ne == "ORG":
        return OrganizationMatcher(longest_match_only=True)
    elif ne == "EVENT":
        return EventMatcher(longest_match_only=True)
    elif ne == "WORK_OF_ART":
        return WorkOfArtMatcher(longest_match_only=True)
    elif ne == "LANGUAGE":
        return LanguageMatcher(longest_match_only=True)