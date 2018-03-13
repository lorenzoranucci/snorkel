from snorkel import SnorkelSession
from snorkel.annotations import load_marginals
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.models import Marginal



def train_model(predicate_resume):
    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]
    cids_query=session.query(candidate_subclass.id). \
        join(Marginal, Marginal.candidate_id==candidate_subclass.id). \
        filter(candidate_subclass.split == 0)
    train_marginals = load_marginals(session, split=0, cids_query=cids_query)

    train_kwargs = {
        'lr':         0.01,
        'dim':        50,
        'n_epochs':   10,
        'dropout':    0.25,
        'print_freq': 1,
        'max_sentence_length': 400
    }

    train_cands = session.query(candidate_subclass).\
        join(Marginal, Marginal.candidate_id==candidate_subclass.id).\
        filter(candidate_subclass.split == 0). \
        order_by(candidate_subclass.id).all()
    # dev_cands   = session.query(candidate_subclass).filter(candidate_subclass.split == 1).order_by(candidate_subclass.id).all()
    # test_cands  = session.query(candidate_subclass).filter(candidate_subclass.split == 2).order_by(candidate_subclass.id).limit(500).all()

    lstm = reRNN(seed=1701, n_threads=4)
    lstm.train(train_cands, train_marginals,  **train_kwargs)

    #p, r, f1 = lstm.score(test_cands, L_gold_test)
    #print("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    #tp, fp, tn, fn = lstm.error_analysis(session, test_cands, L_gold_test)


    lstm.save(predicate_resume["predicate_name"])
    #lstm.save_marginals(session, test_cands)

