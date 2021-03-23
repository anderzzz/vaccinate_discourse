'''Bla bla

'''
from datetime import datetime
import pandas as pd

from gdelt import gdelt_access_consts_english, gdelt_access_consts_translation, GDELTAccessor, gdelt_meta

def filter_vaccine(line):
    return ('HEALTH_VACCINATION' in line) or ('WB_1459_IMMUNIZATIONS' in line)

gdelt_accessor = GDELTAccessor(line_filter_func=filter_vaccine,
                               accessor_consts=gdelt_access_consts_english,
                               metadata=gdelt_meta,
                               dt_start=datetime(year=2019, month=1, day=1, hour=0, minute=0),
                               dt_end=datetime(year=2019, month=1, day=7, hour=0, minute=0)
                               )

bb = []
for pd_snippet in gdelt_accessor:
    print (pd_snippet)
    bb.append(pd_snippet)

df = pd.concat(bb)
df.to_pickle('tester.zip', compression='zip')