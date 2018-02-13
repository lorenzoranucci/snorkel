import os
from os import listdir
from os.path import isfile, join

# TO USE A DATABASE OTHER THAN SQLITE, USE THIS LINE
# Note that this is necessary for parallel execution amongst other things...
#os.environ['SNORKELDB'] = 'postgresql://sentimantic:sentimantic@localhost:5432/sentimantic'
from snorkel import SnorkelSession
session = SnorkelSession()

from snorkel.parser import XMLMultiDocPreprocessor
from snorkel.parser.spacy_parser import Spacy
from snorkel.parser import CorpusParser

dumpsFolderPath='../../data/wikipedia/dump/en/extracted_text/split_dafare/'
corpus_parser = CorpusParser(parser=Spacy())
onlyfiles = [f for f in listdir(dumpsFolderPath) if isfile(join(dumpsFolderPath, f))]

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
    #if i==0 : clear=True


    corpus_parser.apply(doc_preprocessor, clear=clear)#, parallelism=4)

    from snorkel.models import Document, Sentence

    print("Documents:", session.query(Document).count())
    print("Sentences:", session.query(Sentence).count())
    i=i+1