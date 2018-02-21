import os
from os import listdir
from os.path import isfile, join
from snorkel import SnorkelSession
from snorkel.parser import XMLMultiDocPreprocessor
from snorkel.parser.spacy_parser import Spacy
from snorkel.parser import CorpusParser


def parse_wikipedia_dump(
    dumps_folder_path='../../data/wikipedia/dump/en/extracted_text/split/'):


    session = SnorkelSession()

    corpus_parser = CorpusParser(parser=Spacy())
    onlyfiles = [f for f in listdir(dumps_folder_path) if isfile(join(dumps_folder_path, f))]

    i=0
    for file in onlyfiles:
        print file
        doc_preprocessor = XMLMultiDocPreprocessor(
            path='../../data/wikipedia/dump/en/extracted_text/split/'+file,
            doc='.//document',
            text='.//text/text()',
            id='.//id/text()'
        )
        clear=False
        parallelism=None
        if 'SNORKELDB' in os.environ and os.environ['SNORKELDB'] != '':
            parallelism=7
        corpus_parser.apply(doc_preprocessor, clear=clear, parallelism=parallelism)

        from snorkel.models import Document, Sentence

        print("Documents:", session.query(Document).count())
        print("Sentences:", session.query(Sentence).count())
        i=i+1