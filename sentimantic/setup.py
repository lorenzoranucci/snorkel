from sqlalchemy.exc import IntegrityError
from models import create_database, get_sentimanctic_session, Predicate, NamedEntity, Type, TypeNamedEntityAssoc

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

predicates_list = ["http://dbpedia.org/ontology/country",
                  "http://dbpedia.org/ontology/birthDate",
                  "http://dbpedia.org/ontology/location",
                  "http://dbpedia.org/ontology/isPartOf",
                  "http://dbpedia.org/ontology/team",
                  "http://dbpedia.org/ontology/deathPlace",
                  "http://dbpedia.org/ontology/almaMater",
                  "http://dbpedia.org/ontology/league",
                  "http://dbpedia.org/ontology/city",
                  "http://dbpedia.org/ontology/starring",
                  "http://dbpedia.org/ontology/party",
                  "http://dbpedia.org/ontology/parent",
                  "http://dbpedia.org/ontology/spouse",
                  "http://dbpedia.org/ontology/award",
                  "http://dbpedia.org/ontology/network",
                  "http://dbpedia.org/ontology/hometown",
                  "http://dbpedia.org/ontology/formerTeam",
                  "http://dbpedia.org/ontology/recordLabel"
                  ]

for predicateURI in predicates_list:
    try:
        new_predicate=Predicate(uri=predicateURI)
        sentimantic_session.add(new_predicate)
        sentimantic_session.commit()
    except IntegrityError:
        print("Integrity error")
        sentimantic_session.rollback()
    # predicateSplit=predicateURI.split('/')
    # predicateSplitLen= len(predicateSplit)
    # predicate=predicateSplit[predicateSplitLen-1]
    # #retrieve predicate domain
    # domains=get_predicate_domains(predicateURI)
    # #retrieve predicate range
    # ranges=get_predicate_ranges(predicateURI)
    # samplesCount=count_predicate_samples(predicateURI)
    # for domain in domains:
    #     subjectType=domain["namedEntityType"]
    #     for range in ranges:
    #         objectType=range["namedEntityType"]



from entityTypes import getNamedEntityType
