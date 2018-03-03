import os
from os import listdir
from os.path import isfile, join
from snorkel import SnorkelSession
from snorkel.parser import XMLMultiDocPreprocessor
from snorkel.parser.spacy_parser import Spacy
from snorkel.parser import CorpusParser
from snorkel.models import Document, Sentence
import logging

def parse_wikipedia_dump(
    dumps_folder_path='../../data/wikipedia/dump/en_1/extracted_text/split/', clear=False):

    logging.info("Corpus parsing start")
    session = SnorkelSession()
    if clear== False:
        doc_count = session.query(Document).count()
        if doc_count > 1:
            logging.warn("Documents already parsed, skipping...")
            return



    corpus_parser = CorpusParser(parser=Spacy())
    onlyfiles = [f for f in listdir(dumps_folder_path) if isfile(join(dumps_folder_path, f))]

    i=0
    for file in onlyfiles:
        print file
        doc_preprocessor = XMLMultiDocPreprocessor(
            path=dumps_folder_path+file,
            doc='.//document',
            text='.//text/text()',
            id='.//id/text()'
        )
        if i > 0:
            clear = False
        parallelism=None
        if 'SNORKELDB' in os.environ and os.environ['SNORKELDB'] != '':
            parallelism=25
        try:
            corpus_parser.apply(doc_preprocessor, clear=clear, parallelism=parallelism)
        except Exception as e :
            logging.warning("Corpus parsing error: %s", e)


        logging.debug("Documents: %d", session.query(Document).count())
        logging.debug("Sentences: %d", session.query(Sentence).count())
        i=i+1
    logging.info("Corpus parsing end")