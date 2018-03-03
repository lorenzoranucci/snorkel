import os
import logging
from snorkel import SnorkelSession
from snorkel.models import candidate_subclass
from snorkel.candidates import Ngrams, CandidateExtractor
from snorkel.matchers import PersonMatcher, DateMatcher,  OrganizationMatcher
from matchers import GPEMatcher, EventMatcher, WorkOfArtMatcher, LanguageMatcher
from snorkel.models import Sentence

def extract_binary_candidates(predicate_resume, clear=False, parallel=True):
    logging.info("Starting candidates extraction ")
    parallelism=None
    if parallel == True and 'SNORKELDB' in os.environ and os.environ['SNORKELDB'] != '':
        parallelism=20
    subject_ne=predicate_resume['subject_ne']
    object_ne=predicate_resume['object_ne']

    session = SnorkelSession()
    CandidateSubclass = predicate_resume["candidate_subclass"]
    candidates_count = session.query(CandidateSubclass).count()
    if candidates_count>1 and clear==False:
        logging.warn("Candidates already extracted, skipping...")
        return

    ngrams= Ngrams(n_max=7)
    subject_matcher = get_matcher(subject_ne)
    object_matcher = get_matcher(object_ne)
    cand_extractor = CandidateExtractor(CandidateSubclass, [ngrams, ngrams], [subject_matcher,object_matcher],self_relations=True,
                                        nested_relations=True,
                                        symmetric_relations=True)

    sents_count=session.query(Sentence).count()

    if sents_count > 100000:
        page=10000
    else:
        page=sents_count/10 #split in 10 chunks
    i=1
    while(True):
        extracted_count=0
        set_name="train"
        split=0
        if i % 10 == 2:
            set_name="dev"
            split=1
        elif i % 10 == 3:
            set_name="test"
            split=2
        else:
            set_name="train"
            split=0

        logging.debug('\tQuering sentences from %s to %s, in set \'%s\'', (page*(i-1))+1, page*i, set_name)
        sents=session.query(Sentence).order_by(Sentence.id).slice((page*(i-1))+1, page*i).all()
        if sents == None or len(sents) < 1 :
            break
        cand_extractor.apply(sents, split=split, clear=clear, progress_bar=False, parallelism=parallelism)
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