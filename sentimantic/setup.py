from sqlalchemy.exc import IntegrityError
from models import create_database, get_sentimanctic_session, NamedEntity, Type, TypeNamedEntityAssoc

create_database()
SentimanticSession=get_sentimanctic_session()
sentimantic_session=SentimanticSession()

ne_type_list = [
                ["PERSON", "http://dbpedia.org/ontology/Person"],
                ["DATE", "http://www.w3.org/2001/XMLSchema#date"],
                ["GPE", "http://dbpedia.org/ontology/Place"],
                ["ORG", "http://dbpedia.org/ontology/Organisation"],
                ["EVENT", "http://dbpedia.org/ontology/Event"]
            ]

for ne_type in ne_type_list:
    try:
        new_ne = NamedEntity(name=ne_type[0])
        sentimantic_session.add(new_ne)
        sentimantic_session.commit()
    except IntegrityError:
        print("Integrity error")
        sentimantic_session.rollback()
    try:
        new_type = Type(uri=ne_type[1])
        sentimantic_session.add(new_type)
        sentimantic_session.commit()
    except IntegrityError:
        print("Integrity error")
        sentimantic_session.rollback()
    try:
        ne=sentimantic_session.query(NamedEntity).filter(NamedEntity.name == ne_type[0]).first()
        type=sentimantic_session.query(Type).filter(Type.uri == ne_type[1]).first()
        type_ne= TypeNamedEntityAssoc(type=type.uri,namedentity=ne.name)
        sentimantic_session.add(type_ne)
        sentimantic_session.commit()
    except IntegrityError:
        print("Integrity error")
        sentimantic_session.rollback()


