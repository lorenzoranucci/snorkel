from snorkel import SnorkelSession
from snorkel.annotations import load_marginals
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.models import Marginal, Document, Sentence, Candidate, Span
from corpus_parser import parse_wikipedia_dump
from candidateExtraction import extract_binary_candidates
from sqlalchemy import desc
from sqlalchemy import or_

def test_model(predicate_resume):


    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]
    test_cands  = session.query(candidate_subclass).\
        filter(or_(candidate_subclass.id==7581,
                   candidate_subclass.id==103456,
                   candidate_subclass.id==9697,
                   candidate_subclass.id==9699,
                   candidate_subclass.id==6810,
                   candidate_subclass.id==7663)).\
        all()

    lstm = reRNN(seed=1701, n_threads=4)
    #lstm.train(train_cands, train_marginals,  **train_kwargs)

    #p, r, f1 = lstm.score(test_cands, L_gold_test)
    #print("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    #tp, fp, tn, fn = lstm.error_analysis(session, test_cands, L_gold_test)


    lstm.load(predicate_resume["predicate_name"])
    predictions=lstm.predictions(test_cands)
    marginals=lstm.marginals(test_cands)
    i=0
    for candidate in test_cands:
        print(candidate.get_parent().text+" || "+str(marginals[i])+" || "+str(predictions[i]))
        i=i+1


def before_test(predicate_resume,test_file_path):
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