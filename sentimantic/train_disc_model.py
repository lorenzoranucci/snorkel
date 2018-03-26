import logging
from time import gmtime, strftime
from snorkel import SnorkelSession
from snorkel.annotations import load_marginals
from snorkel.learning.disc_models.rnn import reRNN
import shutil
from models import *



def train_disc_model(predicate_resume, parallelism=8):
    logging.info("Start training disc ")
    session = SnorkelSession()
    train_cids_query = get_train_cids_with_marginals_and_span(predicate_resume, session)
    logging.info("Loading marginals ")
    train_marginals = load_marginals(session, split=0, cids_query=train_cids_query)

    train_kwargs = {
        'lr':         0.01,
        'dim':        50,
        'n_epochs':   1,
        'dropout':    0.25,
        'print_freq': 1,
        'max_sentence_length': 400
    }

    logging.info("Querying train cands")
    train_cands = get_train_cands_with_marginals_and_span(predicate_resume, session).all()
    logging.info("Querying dev cands")
    dev_cands = get_dev_cands_with_span(predicate_resume, session).all()
    logging.info("Querying gold labels")
    L_gold_dev = get_gold_dev_matrix(predicate_resume, session)
    logging.info("Training")
    lstm = reRNN(seed=1701, n_threads=int(parallelism))
    lstm.train(train_cands, train_marginals, X_dev=dev_cands, Y_dev=L_gold_dev, **train_kwargs)
    logging.info("Saving")
    _save_model(predicate_resume, lstm)



def _save_model(predicate_resume,lstm):
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())
    lstm.save("D"+predicate_resume["predicate_name"]+date_time)
    try:
        shutil.rmtree("./checkpoints/"+"D"+predicate_resume["predicate_name"]+"Latest")
    except:
        print("Latest model not found, not removed")
    lstm.save("D"+predicate_resume["predicate_name"]+"Latest")
    logging.info("End training disc ")

