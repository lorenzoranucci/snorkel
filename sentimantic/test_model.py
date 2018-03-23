import logging
from snorkel import SnorkelSession
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.models import Sentence, Candidate
from corpus_parser import parse_wikipedia_dump
from candidate_extraction import extract_binary_candidates
from sqlalchemy import desc
from snorkel.annotations import load_gold_labels
from sqlalchemy import or_

def test_model(predicate_resume, model_name=None, limit=None):
    # Todo controlla che minchia hai committato iersera

    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]

    test_cands_query  = session.query(candidate_subclass).filter(candidate_subclass.split == 2).order_by(candidate_subclass.id)
    L_gold_test = load_gold_labels(session, annotator_name='gold', split=2)

    if limit is not None:
        test_cands_query.limit(limit)
    test_cands=test_cands_query.all()
    # filter(or_(candidate_subclass.id==7581,
        #            candidate_subclass.id==103456,
        #            candidate_subclass.id==9697,
        #            candidate_subclass.id==9699,
        #            candidate_subclass.id==6810,
        #            candidate_subclass.id==7663)).\


    lstm = reRNN()

    if model_name is None:
        model_name="D"+predicate_resume["predicate_name"]+"Latest"
    lstm.load(model_name)
    p, r, f1 = lstm.score(test_cands, L_gold_test)
    print("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    tp, fp, tn, fn = lstm.error_analysis(session, test_cands, L_gold_test)
    lstm.save_marginals(session, test_cands)


def before_test(predicate_resume,test_file_path=".data/test.xml"):
    session = SnorkelSession()
    count_sentences=session.query(Sentence).count()
    parse_wikipedia_dump(test_file_path)
    count_sentences2=session.query(Sentence).count()
    count_parsed_sentences=count_sentences2-count_sentences
    sents_query=session.query(Sentence).order_by(desc(Sentence.id)).slice(1,count_parsed_sentences)

    count_candidates=session.query(Candidate).filter(Candidate.split==3).count()
    extract_binary_candidates(predicate_resume=predicate_resume,sents_query=sents_query, split=3)
    count_candidates2=session.query(Candidate).filter(Candidate.split==3).count()
    count_candidates_extracted=count_candidates2-count_candidates
    candidates=sents_query=session.query(Candidate).order_by(desc(Candidate.id)).slice(1,count_candidates_extracted).all()
    return candidates