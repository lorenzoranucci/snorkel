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
    score_lfs(predicate_resume,session)
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

def score_lfs(predicate_resume, session):
    L_train = load_ltrain(predicate_resume,session)
    logging.info(L_train.lf_stats(session))
    print(L_train.lf_stats(session))

def load_ltrain(predicate_resume, session):
    key_group=predicate_resume["label_group"]
    LFs = get_labelling_functions(predicate_resume)
    labeler = LabelAnnotator(lfs=LFs)
    train_cids_query=get_train_cids_with_span(predicate_resume,session)
    L_train = labeler.load_matrix(session,  cids_query=train_cids_query, key_group=key_group)
    return L_train

def score_gen_model(predicate_resume, session, gen_model_name=None, parallelism=8):
    if gen_model_name is None:
        model_name="G"+predicate_resume["predicate_name"]+"Latest"
    logging.info("Stats logging")
    key_group=predicate_resume["label_group"]
    train_cids_query=get_train_cids_with_span(predicate_resume,session)
    L_train = load_ltrain(predicate_resume,session)
    gen_model = GenerativeModel()
    gen_model.load(model_name)
    gen_model.train(L_train, epochs=100, decay=0.95, step_size=0.1 / L_train.shape[0], reg_param=1e-6)
    logging.info(gen_model.weights.lf_accuracy)
    print(gen_model.weights.lf_accuracy)
    train_marginals = gen_model.marginals(L_train)
    #plt.hist(train_marginals, bins=20)
    #plt.show()
    gen_model.learned_lf_stats()



    LFs = get_labelling_functions(predicate_resume)
    labeler = LabelAnnotator(lfs=LFs)
    L_dev = labeler.apply_existing(cids_query=get_dev_cids_with_span(predicate_resume,session))
    L_gold_dev = get_gold_dev_matrix(predicate_resume,session)
    tp, fp, tn, fn = gen_model.error_analysis(session, L_dev, L_gold_dev)
    logging.info("TP: {}, FP: {}, TN: {}, FN: {}".format(str(len(tp)),
                                                         str(len(fp)),
                                                         str(len(tn)),
                                                         str(len(fn))))







