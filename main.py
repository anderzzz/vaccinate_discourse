'''Bla bla

'''
from datetime import datetime
import pandas as pd

from gdelt import gdelt_access_consts_english, gdelt_access_consts_translation, GDELTAccessor, gdelt_meta

#def filter_vaccine(line):
#    return ('HEALTH_VACCINATION' in line) or ('WB_1459_IMMUNIZATIONS' in line)
def filter_suez(line):
    return 'suez' in line.lower()

gdelt_accessor = GDELTAccessor(line_filter_func=filter_suez,
                               accessor_consts=gdelt_access_consts_english,
                               metadata=gdelt_meta,
                               dt_start=datetime(year=2021, month=3, day=22, hour=12, minute=0),
                               dt_end=datetime(year=2021, month=3, day=25, hour=7, minute=30)
                               )

current_week = None
buffer = []
for dt_current, pd_snippet in gdelt_accessor:
    print (dt_current)
    if current_week is None:
        current_week = dt_current.isocalendar()[1]
    else:
        if current_week != dt_current.isocalendar()[1]:
            df = pd.concat(buffer)
            df.to_csv('./data/gdelt_eng_suez_{}_{}.csv'.format(dt_current.year, current_week))
            current_week = dt_current.isocalendar()[1]
            buffer = []

    buffer.append(pd_snippet)

df = pd.concat(buffer)
df.to_csv('./data/gdelt_eng_suez_{}_{}.csv'.format(dt_current.year, current_week))
