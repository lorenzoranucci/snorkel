from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

SentimanticBase = declarative_base(name='SentimanticBase', cls=object)

def get_sentimantic_engine():
    sentimantic_engine=create_engine('postgresql://sentimantic:sentimantic@postgres:5432/sentimantic')
    return sentimantic_engine

class Predicate(SentimanticBase):
    __tablename__ = 'predicate'
    id = Column(Integer, primary_key=True)
    uri = Column(String(2000), nullable=False, unique=True)


class Type(SentimanticBase):
    __tablename__ = 'type'
    uri = Column(String(2000), primary_key=True)


# class PredicateTypesAssoc(SentimanticBase):
#     __tablename__ = 'predicate_type_assoc'
#     id = Column(Integer, primary_key=True)
#     predicate_id= Column(Integer, ForeignKey('predicate.id', ondelete='CASCADE'), nullable=False)
#     subject_type = Column(Integer, ForeignKey('type.id', ondelete='CASCADE'), nullable=False)
#     object_type = Column(Integer, ForeignKey('type.id', ondelete='CASCADE'), nullable=False)
#     UniqueConstraint('subject_type', 'object_type')


class NamedEntity(SentimanticBase):
    __tablename__ = 'namedentity'
    name = Column(String,  primary_key=True)


class TypeNamedEntityAssoc(SentimanticBase):
    __tablename__ = 'type_namedentity_assoc'
    type =  Column(String, ForeignKey('type.uri', ondelete='CASCADE'), primary_key=True)
    namedentity = Column(String, ForeignKey('namedentity.name', ondelete='CASCADE'), primary_key=True)

#all candidates list
class BinaryCandidate(SentimanticBase):
    __tablename__ = 'binarycandidate'
    id = Column(Integer, primary_key=True)
    subject_namedentity = Column(String, ForeignKey('namedentity.name', ondelete='CASCADE'))
    object_namedentity = Column(String, ForeignKey('namedentity.name', ondelete='CASCADE'))
    UniqueConstraint('subject_namedentity', 'object_namedentity')


#binary candidate for each predicate
class PredicateCandidateAssoc(SentimanticBase):
    __tablename__ = 'predicate_candidate_assoc'
    id = Column(Integer, primary_key=True)
    predicate_id= Column(Integer, ForeignKey('predicate.id', ondelete='CASCADE'), nullable=False)
    candidate_id= Column(Integer, ForeignKey('binarycandidate.id', ondelete='CASCADE'), nullable=False)
    samples_file_path= Column(String(2000))
    UniqueConstraint('predicate_id', 'candidate_id')



def get_sentimanctic_session():
    return sessionmaker(bind=get_sentimantic_engine())

def create_database():
    engine=get_sentimantic_engine()
    #SentimanticBase.metadata.create_all(engine)
    statement = text("""INSERT INTO type (uri) VALUES ('http://www.w3.org/2001/XMLSchema#date');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Place');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Organisation');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Event');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Work');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Language');
INSERT INTO type (uri) VALUES ('http://dbpedia.org/ontology/Award');
INSERT INTO namedentity (name) VALUES ('PERSON');
INSERT INTO namedentity (name) VALUES ('DATE');
INSERT INTO namedentity (name) VALUES ('GPE');
INSERT INTO namedentity (name) VALUES ('ORG');
INSERT INTO namedentity (name) VALUES ('EVENT');
INSERT INTO namedentity (name) VALUES ('WORK_OF_ART');
INSERT INTO namedentity (name) VALUES ('LANGUAGE');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Person', 'PERSON');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://www.w3.org/2001/XMLSchema#date', 'DATE');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Place', 'GPE');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Organisation', 'ORG');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Event', 'EVENT');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Work', 'WORK_OF_ART');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Language', 'LANGUAGE');
INSERT INTO type_namedentity_assoc (type, namedentity) VALUES ('http://dbpedia.org/ontology/Award', 'ORG');
""")
    engine.execute(statement)