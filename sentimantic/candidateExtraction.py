import os


from snorkel import SnorkelSession
from snorkel.models import candidate_subclass
from snorkel.candidates import Ngrams, CandidateExtractor
from snorkel.matchers import PersonMatcher, DateMatcher,  OrganizationMatcher
from matchers import GPEMatcher, EventMatcher, WorkOfArtMatcher, LanguageMatcher
from snorkel.models import Sentence
import logging

def extract_binary_candidates(candidates):
    logging.info("Candidates extraction start")
    parallelism=None
    if 'SNORKELDB' in os.environ and os.environ['SNORKELDB'] != '':
        parallelism=20
    candidate_list=[]
    for candidate in candidates:
        subject_ne=candidate.subject_namedentity
        object_ne=candidate.object_namedentity

        session = SnorkelSession()
        candidate_name=(subject_ne+object_ne).encode("utf-8")

        CandidateSubclass = candidate_subclass(candidate_name,
                                               ["subject_"+subject_ne.lower(),
                                                "object_"+object_ne.lower()
                                                ])
        ngrams= Ngrams(n_max=7)
        subject_matcher = get_matcher(subject_ne)
        object_matcher = get_matcher(object_ne)
        cand_extractor = CandidateExtractor(CandidateSubclass, [ngrams, ngrams], [subject_matcher,object_matcher])
        candidate_list.append({"extractor":cand_extractor,"subclass":CandidateSubclass})

    clear=True
    page=200000
    start=1
    i=1
    while(True):
        extracted_count=0
        set_name="train"
        split=1
        if i % 10 == 8:
            set_name="dev"
            split=2
        elif i % 10 == 9:
            set_name="test"
            split=3
        else:
            set_name="train"
            split=1

        logging.debug('\tQuering sentences from %s to %s, in set \'%s\'', start, start+page, set_name)
        sents=session.query(Sentence).order_by(Sentence.id).slice(start,start+page).all()
        if sents == None or len(sents) < 1 : break
        for candidate in candidate_list:
            candidate["extractor"].apply(sents, split=split, clear=clear, progress_bar=False, parallelism=parallelism)
            extracted_count=session.query(candidate["subclass"]).filter(candidate["subclass"].split == split).count()
            logging.debug('\t\t%d candidates extracted for %s', extracted_count, candidate["subclass"].__name__)
        start=start+page
        clear=False
        i=i+1



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