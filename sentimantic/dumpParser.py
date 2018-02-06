import os
from os import listdir
from os.path import isfile, join

from snorkel import SnorkelSession
session = SnorkelSession()

from snorkel.parser import XMLMultiDocPreprocessor
from snorkel.parser.spacy_parser import Spacy
from snorkel.parser import CorpusParser

#texts dbpedia nif format nlp http://wiki.dbpedia.org/downloads-2016-10
corpus_parser = CorpusParser(parser=Spacy())
onlyfiles = [f for f in listdir('../../data/wikipedia/dump/en/extracted_text/split/') if isfile(join('../../data/wikipedia/dump/en/extracted_text/split/', f))]

i=0
for file in onlyfiles:
    doc_preprocessor = XMLMultiDocPreprocessor(
        path='../../data/wikipedia/dump/en/extracted_text/split/'+file,
        doc='.//document',
        text='.//text/text()',
        id='.//id/text()'
    )
    clear=False
    #if i==0 : clear=True


    corpus_parser.apply(doc_preprocessor, clear=clear)

    from snorkel.models import Document, Sentence

    print("Documents:", session.query(Document).count())
    print("Sentences:", session.query(Sentence).count())
    i=i+1