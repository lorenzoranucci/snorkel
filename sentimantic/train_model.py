from time import gmtime, strftime
from snorkel import SnorkelSession
from snorkel.annotations import load_marginals, LabelAnnotator
from snorkel.learning import GenerativeModel
from snorkel.learning.disc_models.rnn import reRNN
from snorkel.models import Marginal
from labelling import get_labelling_functions
from snorkel.annotations import save_marginals
#import matplotlib.pyplot as plt
import shutil



def train_model(predicate_resume, parallelism=8, words={}):
    date_time=strftime("%d-%m-%Y_%H_%M_%S", gmtime())

    session = SnorkelSession()
    candidate_subclass=predicate_resume["candidate_subclass"]
    cids_query=session.query(candidate_subclass.id).filter(candidate_subclass.split == 0)
    key_group=predicate_resume["label_group"]
    LFs = get_labelling_functions(predicate_resume)
    labeler = LabelAnnotator(lfs=LFs)
    L_train = labeler.load_matrix(session,  cids_query=cids_query, key_group=key_group)
    gen_model = GenerativeModel()
    gen_model.train(L_train, epochs=100, decay=0.95, step_size=0.1 / L_train.shape[0], reg_param=1e-6, threads=8)
    gen_model.save("G"+predicate_resume["predicate_name"]+date_time)
    try:
        shutil.rmtree("./checkpoints/"+"D"+predicate_resume["predicate_name"]+"Latest")
    except:
        print("Latest model not found, not removed")
    gen_model.save("G"+predicate_resume["predicate_name"]+"Latest")

    train_marginals = gen_model.marginals(L_train)
    #plt.hist(train_marginals, bins=20)
    #plt.show()
    #print(gen_model.learned_lf_stats())
    save_marginals(session, L_train, train_marginals)
    # gen_model.weights.lf_accuracy
    #print(L_train.get_candidate(session, 0))
    #print(L_train.get_key(session, 0))
    #print(L_train.lf_stats(session))

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

    lstm = reRNN(seed=1701, n_threads=int(parallelism))
    lstm.train(train_cands, train_marginals,  **train_kwargs)

    #p, r, f1 = lstm.score(test_cands, L_gold_test)
    #print("Prec: {0:.3f}, Recall: {1:.3f}, F1 Score: {2:.3f}".format(p, r, f1))
    #tp, fp, tn, fn = lstm.error_analysis(session, test_cands, L_gold_test)


    lstm.save("D"+predicate_resume["predicate_name"]+date_time)
    try:
        shutil.rmtree("./checkpoints/"+"D"+predicate_resume["predicate_name"]+"Latest")
    except:
        print("Latest model not found, not removed")
    lstm.save("D"+predicate_resume["predicate_name"]+"Latest")
    #lstm.save_marginals(session, test_cands)

