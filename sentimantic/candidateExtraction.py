from snorkel import SnorkelSession
from snorkel.models import candidate_subclass
from snorkel.candidates import Ngrams, CandidateExtractor
from snorkel.matchers import PersonMatcher, DateMatcher,  OrganizationMatcher
from matchers import GPEMatcher, EventMatcher, WorkOfArtMatcher, LanguageMatcher

def extract_binary_candidate(subject_ne, object_ne):
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

    from snorkel.models import Document

    docs = session.query(Document).order_by(Document.name).all()


    for i, doc in enumerate(docs):
        if i % 10 == 8:
            cand_extractor.apply(doc.sentences, split=2, clear=False)
        elif i % 10 == 9:
            cand_extractor.apply(doc.sentences, split=3, clear=False)
        else:
            cand_extractor.apply(doc.sentences, split=1, clear=False)


    print("Number of train candidates:", session.query(CandidateSubclass).filter(CandidateSubclass.split == 1).count())
    print("Number of dev candidates:", session.query(CandidateSubclass).filter(CandidateSubclass.split == 2).count())
    print("Number of test candidates:", session.query(CandidateSubclass).filter(CandidateSubclass.split == 3).count())


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