import logging
from snorkel import SnorkelSession
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.annotations import  LabelAnnotator
from snorkel.learning import GenerativeModel
from labelling import get_labelling_functions
from models import *
import matplotlib.pyplot as plt

def test_model(predicate_resume,gen_model_name=None, disc_model_name=None):
    session = SnorkelSession()
    score_disc_model(predicate_resume,session,disc_model_name)
    score_gen_model(predicate_resume,session,gen_model_name)



def score_disc_model(predicate_resume, session, disc_model_name=None):
    if disc_model_name is None:
        model_name="D"+predicate_resume["predicate_name"]+"Latest"

    candidate_subclass=predicate_resume["candidate_subclass"]
    test_cands_query  = session.query(candidate_subclass).filter(candidate_subclass.split == 2).order_by(candidate_subclass.id)
    L_gold_test = get_gold_test_matrix(predicate_resume,session)
    test_cands=test_cands_query.all()
    lstm = reRNN()
    lstm.load(model_name)
    lstm.save_marginals(session, test_cands)
    p, r, f1 = lstm.score(test_cands, L_gold_test)
    print("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    logging.info("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    tp, fp, tn, fn = lstm.error_analysis(session, test_cands, L_gold_test)
    logging.info("TP: {}, FP: {}, TN: {}, FN: {}".format(str(len(tp)),
                                                         str(len(fp)),
                                                         str(len(tn)),
                                                         str(len(fn))))


    #predictions=lstm.predictions(test_cands)
    #marginals=lstm.marginals(test_cands)
    #i=0
    #for candidate in test_cands:
    #    print(candidate.get_parent().text+" || "+str(marginals[i])+" || "+str(predictions[i]))
    #    logging.info(candidate.get_parent().text+" || "+str(marginals[i])+" || "+str(predictions[i]))
    #    i=i+1



def score_gen_model(predicate_resume, session, gen_model_name=None, parallelism=8):
    if gen_model_name is None:
        model_name="G"+predicate_resume["predicate_name"]+"Latest"
    logging.info("Stats logging")
    key_group=predicate_resume["label_group"]
    train_cids_query=get_train_cids_with_span(predicate_resume,session)

    LFs = get_labelling_functions(predicate_resume)
    logging.info("Get marginals")
    labeler = LabelAnnotator(lfs=LFs)
    logging.info("Load matrix")
    L_train = labeler.load_matrix(session,  cids_query=train_cids_query, key_group=key_group)
    gen_model = GenerativeModel()
    gen_model.load(model_name=model_name)
    train_marginals = gen_model.marginals(L_train)


    logging.info("Saving marginals")
    candidate_subclass=predicate_resume["candidate_subclass"]
    dev_cands_query  = session.query(candidate_subclass).filter(candidate_subclass.split == 1).order_by(candidate_subclass.id)
    dev_cands=dev_cands_query.all()
    gen_model.save_marginals(session, dev_cands)


    logging.info("Applying ")
    test_cids_query=get_test_cids_with_span(predicate_resume,session)
    L_gold_test = get_gold_test_matrix(predicate_resume,session)
    L_test = labeler.apply_existing(parallelism=parallelism, cids_query=test_cids_query,
                                   key_group=key_group, clear=False)

    tp, fp, tn, fn = gen_model.error_analysis(session, L_test, L_gold_test)
    logging.info("TP: {}, FP: {}, TN: {}, FN: {}".format(str(len(tp)),
                                                         str(len(fp)),
                                                         str(len(tn)),
                                                         str(len(fn))))
    logging.info("\n"+L_train.lf_stats(session))
    plt.hist(train_marginals, bins=20)
    plt.show()
