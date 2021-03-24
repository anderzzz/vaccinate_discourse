'''Bla bla

'''
from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT DocumentIdentifier, Themes, Locations, Date, Organizations, V2Tone, GCAM 
FROM `gdelt-bq.gdeltv2.gkg`
WHERE (REGEXP_CONTAINS(Themes, r"VACCINATION") OR REGEXP_CONTAINS(Themes, r"WB_1459_IMMUNIZATIONS")) 
    AND (Date>20210320000000) 
LIMIT 10;
"""

#query = """
#    SELECT name, SUM(number) as total_people
#    FROM `bigquery-public-data.usa_names.usa_1910_2013`
#    WHERE state = 'TX'
#    GROUP BY name, state
#    ORDER BY total_people DESC
#    LIMIT 20
#"""

query_job = client.query(query)

print ('The query data:')
for row in query_job:
    print (row)