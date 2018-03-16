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
    dumps_folder_path='../../data/wikipedia/dump/en/extracted_text/AA/', clear=False, parallelism=1):

    logging.info("Corpus parsing start")
    session = SnorkelSession()



    corpus_parser = CorpusParser(parser=Spacy())
    onlyfiles = [f for f in listdir(dumps_folder_path) if isfile(join(dumps_folder_path, f))]

    i=0
    for file in onlyfiles:
        if  file.endswith(".xml"):
            print file
            doc_preprocessor = XMLMultiDocPreprocessor(
                path=dumps_folder_path+file,
                doc='.//doc',
                text='./text()',
                id='./@title'
            )
            if i > 0:
                clear = False
            corpus_parser.apply(doc_preprocessor, clear=clear, parallelism=parallelism)
            i=i+1
    logging.debug("Documents: %d", session.query(Document).count())
    logging.debug("Sentences: %d", session.query(Sentence).count())
    logging.info("Corpus parsing end")