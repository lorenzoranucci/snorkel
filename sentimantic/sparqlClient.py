from SPARQLWrapper import SPARQLWrapper, JSON
sparql = SPARQLWrapper("https://dbpedia.org/sparql")

listOfPredicates=["http://dbpedia.org/ontology/country","http://dbpedia.org/ontology/birthDate"]
for predicateURI in listOfPredicates:
    predicateSplit=predicateURI.split('/')
    predicateSplitLen= len(predicateSplit)
    predicate=predicateSplit[predicateSplitLen-1]

    #retrieve predicate domain
    domainQuery="""
    SELECT DISTINCT ?domain
    WHERE {
        <"""+predicateURI+"""> <http://www.w3.org/2000/01/rdf-schema#domain> ?domain.
    }"""
    sparql.setQuery(domainQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    resultsCount=len(results["results"]["bindings"])
    domainCount=0
    domains=[]
    for result in results["results"]["bindings"]:
        domains.append([result["domain"]["value"].encode('utf-8').strip(),result["domain"]["value"]])
        domainCount=domainCount+1

    #retrieve predicate range
    rangeQuery="""
    SELECT DISTINCT ?range
    WHERE {
        <"""+predicateURI+"""> <http://www.w3.org/2000/01/rdf-schema#range> ?range.
    }"""
    sparql.setQuery(rangeQuery)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    resultsCount=len(results["results"]["bindings"])
    rangeCount=0
    ranges=[]
    for result in results["results"]["bindings"]:
        ranges.append([result["range"]["value"].encode('utf-8').strip(),result["range"]["value"]])
        rangeCount=rangeCount+1

    for domain in domains:
        subjectType=""
        if domain[0]=='http://dbpedia.org/ontology/Person':subjectType="Person"
        elif domain[0]=='http://www.w3.org/2001/XMLSchema#date': subjectType="Date"
        elif domain[0]=='http://dbpedia.org/ontology/Country': subjectType="Location"
        for range in ranges:
            objectType=""
            if range[0]=='http://dbpedia.org/ontology/Person':objectType="Person"
            elif range[0]=='http://www.w3.org/2001/XMLSchema#date': objectType="Date"
            elif range[0]=='http://dbpedia.org/ontology/Country': objectType="Location"

            #create file for output
            name=predicate+subjectType+objectType
            fn="./data/"+name+".csv"
            file = open(fn, 'a+')
            import csv
            writer = csv.writer(file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            #build query
            querySelect="""
            SELECT DISTINCT ?subjectLabel ?objectLabel \n
            """
            queryWhere="""
            WHERE{
                ?s <"""+predicateURI+"""> ?objectLabel.
                ?s a <"""+domain[1]+""">.
                ?s <http://www.w3.org/2000/01/rdf-schema#label> ?subjectLabel .
                FILTER (lang(?subjectLabel) = 'en')
            }\n
            """
            offset=0
            queryOffset="OFFSET "
            queryLimit="LIMIT 10000"
            resultsCount=1
            while resultsCount > 0  & offset < 20000:
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

            import bz2
            from shutil import copyfileobj
            with bz2.BZ2File(fn+'.bz2', 'wb', compresslevel=9) as output:
                copyfileobj(file, output)

            import os
            os.remove(file.name)


