import os
from SPARQLWrapper import SPARQLWrapper, JSON

subjectType="Person"
objectType="Date"
predicate="birthdate"
name=predicate+subjectType+predicate
#create file for output
fn="./data/"+name+".csv"

file = open(fn, 'w')

import csv
writer = csv.writer(file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

sparql = SPARQLWrapper("https://dbpedia.org/sparql")

querySelect="""
SELECT  ?subjectLabel ?objectLabel \n
"""
queryWhere="""
WHERE{
    ?s <http://dbpedia.org/ontology/birthDate> ?objectLabel.
    ?s a <http://xmlns.com/foaf/0.1/Person>.
    ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
    FILTER (lang(?subjectLabel) = 'en')
}\n
"""
offset=0
queryOffset="OFFSET "
queryLimit="LIMIT 10000"
resultsCount=1
while resultsCount > 0 :
    query=querySelect+queryWhere+queryOffset+str(offset)+" \n"+queryLimit
    print(query)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    resultsCount=len(results["results"]["bindings"])
    for result in results["results"]["bindings"]:
        try:
            subject=result["subjectLabel"]["value"].encode('utf-8').strip().replace(",","").replace("\"","")
            object=result["objectLabel"]["value"].encode('utf-8').strip().replace(",","").replace("\"","")
            writer.writerow([subject,object])
        except Exception as e : print(e)
    offset+=resultsCount


