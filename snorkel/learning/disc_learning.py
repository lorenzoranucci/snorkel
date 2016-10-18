from .constants import *
import numpy as np
from scipy.optimize import minimize
from .utils import score, odds_to_prob
from lstm import LSTMModel
from sklearn import linear_model
from ..models import Parameter, ParameterSet


class NoiseAwareModel(object):
    """Simple abstract base class for a model."""
    def __init__(self, bias_term=False):
        self.w         = None
        self.bias_term = bias_term
        self.X_train   = None

    def train(self, X, training_marginals, **hyperparams):
        """Trains the model; also must set self.X_train and self.w"""
        raise NotImplementedError()

    def marginals(self, X):
        raise NotImplementedError()

    def predict(self, X, b=0.5):
        """Return numpy array of elements in {-1,0,1} based on predicted marginal probabilities."""
        return np.array([1 if p > b else -1 if p < b else 0 for p in self.marginals(X)])

    def score(self, X_test, L_test, gold_candidate_set, b=0.5, set_unlabeled_as_neg=True, display=True):
        if L_test.shape[1] != 1:
            raise ValueError("L_test must have exactly one column.")
        predict = self.predict(X_test, b=b)
        train_marginals = self.marginals(self.X_train) if hasattr(self, 'X_train') and self.X_train is not None else None
        test_marginals = self.marginals(X_test)

        test_candidates = set()
        test_labels = []
        tp = set()
        fp = set()
        tn = set()
        fn = set()

        for i in range(X_test.shape[0]):
            candidate = X_test.get_candidate(i)
            test_candidates.add(candidate)
            try:
                L_test_index = L_test.get_row_index(candidate)
                test_label   = L_test[L_test_index, 0]

                # Set unlabeled examples to -1 by default
                if test_label == 0 and set_unlabeled_as_neg:
                    test_label = -1
              
                # Bucket the candidates for error analysis
                test_labels.append(test_label)
                if test_marginals[i] > b:
                    if test_label == 1:
                        tp.add(candidate)
                    else:
                        fp.add(candidate)
                else:
                    if test_label == -1:
                        tn.add(candidate)
                    else:
                        fn.add(candidate)
            except KeyError:
                test_labels.append(-1)
                if test_marginals[i] > b:
                    fn.add(candidate)
                else:
                    tn.add(candidate)

        # Print diagnostics chart and return error analysis candidate sets
        if display:
            score(test_candidates, np.asarray(test_labels), np.asarray(predict), gold_candidate_set,
                  train_marginals=train_marginals, test_marginals=test_marginals)
        return tp, fp, tn, fn

    def save(self, session, param_set_name):
        """Save the Parameter (weight) values, i.e. the model, as a new ParameterSet"""
        # Check for X_train and w
        if not hasattr(self, 'X_train') or self.X_train is None or not hasattr(self, 'w') or self.w is None:
            name = self.__class__.__name__
            raise Exception("{0}.train() must be run, and must set {0}.X_train and {0}.w".format(name))

        # Create new named ParameterSet
        param_set = ParameterSet(name=param_set_name)
        session.add(param_set)

        # Create and save a new set of Parameters- note that PK of params is (feature_key_id, param_set_id)
        # Note: We can switch to using bulk insert if this is too slow...
        for j, v in enumerate(self.w):
            session.add(Parameter(feature_key_id=self.X_train.col_index[j], set=param_set, value=v))
        session.commit()

    def load(self, session, param_set_name):
        """Load the Parameters into self.w, given ParameterSet.name"""
        q = session.query(Parameter.value).join(ParameterSet).filter(ParameterSet.name == param_set_name)
        q = q.order_by(Parameter.feature_key_id)
        self.w = np.array([res[0] for res in q.all()])


class LogRegSKLearn(NoiseAwareModel):
    """Logistic regression."""
    def __init__(self):
        self.w         = None

    def train(self, X, training_marginals, alpha=1, C=1.0):
        self.X_train = X
        penalty      = 'l1' if alpha == 1 else 'l2'
        self.model   = linear_model.LogisticRegression(penalty=penalty, C=C, dual=False)
       
        # First, we remove the rows (candidates) that have no LF coverage
        covered            = np.where(np.abs(training_marginals - 0.5) > 1e-3)[0]
        training_marginals = training_marginals[covered]
        
        # TODO: This should be X_train, however copy is broken here!
        X = X[covered]

        # Hard threshold the training marginals
        ypred = np.array([1 if x > 0.5 else 0 for x in training_marginals])
        self.model.fit(X, ypred)
        self.w = self.model.coef_.flatten()
    
    def marginals(self, X):
        m = self.model.predict_proba(X)
        return self.model.predict_proba(X)[...,1]


class LogReg(NoiseAwareModel):
    def __init__(self, bias_term=False):
        self.w         = None
        self.bias_term = bias_term

    def _loss(self, X, w, m_t, mu, alpha):
        """
        Our noise-aware loss function (ignoring regularization term):
        L(w) = sum_{x,y} E[ log( 1 + exp(-x^Twy) ) ]
             = sum_{x,y} P(y=1) log( 1 + exp(-x^Tw) ) + P(y=-1) log( 1 + exp(x^Tw) )
        """
        z = X.dot(w)
        return m_t.dot(np.log(1 + np.exp(-z))) + (1 - m_t).dot(np.log(1 + np.exp(z))) \
                + mu * (alpha*np.linalg.norm(w, ord=1) + (1-alpha)*np.linalg.norm(w, ord=2))

    def train(self, X, training_marginals, method='GD', n_iter=1000, w0=None, rate=0.001, backtracking=False, beta=0.8,
              mu=1e-6, alpha=0.5, rate_decay=0.999, hard_thresh=False):
        self.X_train = X

        # First, we remove the rows (candidates) that have no LF coverage
        covered            = np.where(np.abs(training_marginals - 0.5) > 1e-3)[0]
        training_marginals = training_marginals[covered]
        
        # TODO: This should be X_train, however copy is broken here!
        X = X[covered]

        # Option to try hard thresholding
        if hard_thresh:
            training_marginals = np.array([1.0 if x > 0.5 else 0.0 for x in training_marginals])
        m_t, m_f = training_marginals, 1-training_marginals
    
        # Set up stuff
        N, M = X.shape
        print "="*80
        print "Training marginals (!= 0.5):\t%s" % N
        print "Features:\t\t\t%s" % M
        print "="*80
        Xt = X.transpose()
        w0 = w0 if w0 is not None else np.zeros(M)

        # Initialize training
        w = w0.copy()
        g = np.zeros(M)

        # Scipy optimize
        if method == 'L-BFGS':
            print "Using L-BFGS-B..."
            func = lambda w : m_t.dot(np.log(odds_to_prob(X.dot(w)))) + m_f.dot(np.log(odds_to_prob(-X.dot(w))))
            self.res = minimize(func, w0, method='L-BFGS-B', options={'disp':True, 'iprint':10})
            self.w = self.res.x

        # Gradient descent
        elif method == 'GD':
            print "Using gradient descent..."
            for step in range(n_iter):
                if step % 100 == 0:
                    print "\tLearning epoch = {}\tStep size = {}".format(step, rate)

                # Compute the gradient step
                """
                Let g(x) = exp(x) / (1 + exp(x)) = 1 / (1 + exp(-x)) = odds_to_prob(x)

                Our noise-aware loss function (ignoring regularization term):
                L(w) = sum_{x,y} E[ log( 1 + exp(-x^Twy) ) ]
                     = sum_{x,y} P(y=1) log( 1 + exp(-x^Tw) ) + P(y=-1) log( 1 + exp(x^Tw) )

                The gradient is thus:
                grad_w = sum_{x,y} P(y=-1) g(x^Tw) x - P(y=1) g(-x^Tw) x
                """
                z = X.dot(w)
                t = odds_to_prob(z)
                g0 = Xt.dot(np.multiply(t, m_f)) - Xt.dot(np.multiply(1-t, m_t))

                # Compute the loss
                L = self._loss(X, w, m_t, mu, alpha)
                if step % 100 == 0:
                    print "\tLoss = {:.6f}\tGradient magnitude = {:.6f}".format(L, np.linalg.norm(g0, ord=2))

                # Momentum term
                g = 0.95*g0 + 0.05*g

                # Backtracking line search
                if backtracking:
                    while self._loss(X, w - rate*g, m_t, mu, alpha) > L - 0.5*rate*np.linalg.norm(w, ord=2)**2:
                        rate *= beta
                else:
                    rate *= rate_decay

                # Update weights
                w -= rate * g

                # Apply elastic net penalty
                w_bias    = w[-1]
                soft      = np.abs(w) - rate * alpha * mu
                ridge_pen = (1 + (1-alpha) * rate * mu)
                w = (np.sign(w)*np.select([soft>0], [soft], default=0)) / ridge_pen
                if self.bias_term:
                    w[-1] = w_bias

            # Return learned weights
            self.w = w

        else:
            raise NotImplementedError()

    def marginals(self, X):
        return odds_to_prob(X.dot(self.w))


class LSTM(NoiseAwareModel):
    """Long Short-Term Memory."""
    def __init__(self):
        self.lstm = None
        self.w = None

    def train(self, training_candidates, training_marginals, **hyperparams):
        self.lstm = LSTMModel(training_candidates, training_marginals)
        self.lstm.train(**hyperparams)

    def marginals(self, test_candidates):
        return self.lstm.test(test_candidates)

    def save(self, session, param_set_name):
        raise NotImplementedError()
