import logging
from snorkel import SnorkelSession
from snorkel.models import Span, Document
from snorkel.candidates import Ngrams, CandidateExtractor
from snorkel.matchers import PersonMatcher, DateMatcher,  OrganizationMatcher
from matchers import GPEMatcher, EventMatcher, WorkOfArtMatcher, LanguageMatcher
from snorkel.models import Sentence


def extract_binary_candidates(predicate_resume, clear=False, parallelism=8,  split=None, documents_titles=None):
    #create span and candidates
    logging.info("Starting candidates extraction ")
    subject_ne=predicate_resume['subject_ne']
    object_ne=predicate_resume['object_ne']

    session = SnorkelSession()
    CandidateSubclass = predicate_resume["candidate_subclass"]


    ngrams= Ngrams(n_max=10)
    subject_matcher = get_matcher(subject_ne)
    object_matcher = get_matcher(object_ne)
    cand_extractor = CandidateExtractor(CandidateSubclass, [ngrams, ngrams], [subject_matcher,object_matcher],self_relations=True,
                                        nested_relations=True,
                                        symmetric_relations=True)

    #skip sentences already extracted
    sents_query= session.query(Sentence)
    candidates_count = session.query(CandidateSubclass).count()
    if documents_titles==None and candidates_count>1 and clear==False:
        #case with already extracted cands
        subquery=session.query(Sentence.id).join(Span, Span.sentence_id==Sentence.id).join(CandidateSubclass,CandidateSubclass.subject_id==Span.id)
        sents_query= session.query(Sentence).filter(~Sentence.id.in_(subquery))
    elif documents_titles != None:
        #Todo remove candidates already extracted for this documents
        subquery=session.query(Document.id).filter(Document.name.in_(documents_titles))
        sents_query=session.query(Sentence).filter(Sentence.document_id.in_(subquery))
    sents_count=sents_query.count()

    if sents_count > 100000:
        page=10000
    else:
        page=sents_count/10 #split in 10 chunks
    i=1
    while(True):
        set_name=""
        if split == None:
            set_name="train"
            split2=0
        else:
            set_name=str(split)
            split2=split

        logging.debug('\tQuering sentences from %s to %s, in set \'%s\'', (page*(i-1))+1, page*i, set_name)
        sents=sents_query.order_by(Sentence.id).slice((page*(i-1))+1, page*i).all()
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