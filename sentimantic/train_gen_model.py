import logging
from time import gmtime, strftime
from snorkel import SnorkelSession
from snorkel.annotations import  LabelAnnotator
from snorkel.learning import GenerativeModel
from labelling import get_labelling_functions
from snorkel.annotations import save_marginals
from snorkel.annotations import load_gold_labels
import matplotlib.pyplot as plt
import shutil



def train_gen_model(predicate_resume, parallelism=8, words={}):
    logging.info("Start train gen")
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())

    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]
    cids_query=session.query(candidate_subclass.id).filter(candidate_subclass.split == 0)
    key_group=predicate_resume["label_group"]
    LFs = get_labelling_functions(predicate_resume)


    labeler = LabelAnnotator(lfs=LFs)

    logging.info("Load matrix")
    L_train = labeler.load_matrix(session,  cids_query=cids_query, key_group=key_group)
    gen_model = GenerativeModel()
    logging.info("Train model")
    gen_model.train(L_train, epochs=100, decay=0.95, step_size=0.1 / L_train.shape[0], reg_param=1e-6, threads=int(parallelism))

    logging.info("Save model")
    gen_model.save("G"+predicate_resume["predicate_name"]+date_time)
    try:
        shutil.rmtree("./checkpoints/"+"D"+predicate_resume["predicate_name"]+"Latest")
    except:
        print("Latest model not found, not removed")
    gen_model.save("G"+predicate_resume["predicate_name"]+"Latest")

    logging.info("Get marginals")
    train_marginals = gen_model.marginals(L_train)
    #plt.hist(train_marginals, bins=20)
    #plt.show()
    #print(gen_model.learned_lf_stats())
    logging.info("Save marginals")
    save_marginals(session, L_train, train_marginals)

    logging.info("Stats logging")
    #logging.info("\n"+gen_model.weights.lf_accuracy)
    #logging.info("\n"+L_train.lf_stats(session))
#
    #L_gold_dev = load_gold_labels(session, annotator_name='gold', split=1)
    #cids_query2=session.query(candidate_subclass.id).filter(candidate_subclass.split == 0)
    #L_dev = labeler.apply_existing(split=1,cids_query=cids_query2)
    #logging.info("\n"+gen_model.error_analysis(session, L_dev, L_gold_dev))
    #logging.info("\n"+L_dev.lf_stats(session, L_gold_dev, gen_model.learned_lf_stats()['Accuracy']))


