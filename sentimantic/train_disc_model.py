import logging
from time import gmtime, strftime
from snorkel import SnorkelSession
from snorkel.annotations import load_marginals
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.models import Marginal, Span
from snorkel.annotations import load_gold_labels
import shutil



def train_disc_model(predicate_resume, parallelism=8):
    logging.info("Start training disc ")
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())


    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]

    #todo remove sentences with missing spans
    subquery=session.query(Span.id)
    cids_query=session.query(candidate_subclass.id). \
        join(Marginal, Marginal.candidate_id==candidate_subclass.id). \
        filter(candidate_subclass.split == 0). \
        filter(candidate_subclass.subject_id.in_(subquery)). \
        filter(candidate_subclass.object_id.in_(subquery))

    logging.info("Loading marginals ")
    train_marginals = load_marginals(session, split=0, cids_query=cids_query)

    train_kwargs = {
        'lr':         0.01,
        'dim':        50,
        'n_epochs':   10,
        'dropout':    0.25,
        'print_freq': 1,
        'max_sentence_length': 400
    }

    logging.info("Querying train cands")
    train_cands = session.query(candidate_subclass).\
        join(Marginal, Marginal.candidate_id==candidate_subclass.id).\
        filter(candidate_subclass.split == 0). \
        filter(candidate_subclass.subject_id.in_(subquery)). \
        filter(candidate_subclass.object_id.in_(subquery)).\
        all()
    logging.info("Querying dev cands")
    dev_cands   = session.query(candidate_subclass).\
        filter(candidate_subclass.split == 1). \
        filter(candidate_subclass.subject_id.in_(subquery)). \
        filter(candidate_subclass.object_id.in_(subquery)). \
        limit(500).all()

    logging.info("Querying gold labels")
    L_gold_dev  = load_gold_labels(session, annotator_name='gold', split=1)

    logging.info("Training")
    lstm = reRNN(seed=1701, n_threads=int(parallelism))
    lstm.train(train_cands, train_marginals, X_dev=dev_cands, Y_dev=L_gold_dev, **train_kwargs)

    logging.info("Saving")
    lstm.save("D"+predicate_resume["predicate_name"]+date_time)
    try:
        shutil.rmtree("./checkpoints/"+"D"+predicate_resume["predicate_name"]+"Latest")
    except:
        print("Latest model not found, not removed")
    lstm.save("D"+predicate_resume["predicate_name"]+"Latest")
    logging.info("End training disc ")


