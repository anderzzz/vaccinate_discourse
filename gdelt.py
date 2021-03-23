'''Support the accessing and parsing of GDELT data from web-server

'''
from dataclasses import dataclass
from typing import List

from datetime import datetime, timedelta
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import pandas as pd


@dataclass
class GDELTAccessConsts:
    url_base : str
    file_suffix : str
    minute_increment : int = 15

    def make_url(self, dt):
        if self.url_base[-1] == '/':
            stuffer = ''
        else:
            stuffer = '/'
        return "{}{}{}.{}".format(self.url_base, stuffer, dt.strftime('%Y%m%d%H%M%S'), self.file_suffix)

gdelt_access_consts_translation = GDELTAccessConsts('http://data.gdeltproject.org/gdeltv2/',
                                                    'translation.gkg.csv.zip')
gdelt_access_consts_english = GDELTAccessConsts('http://data.gdeltproject.org/gdeltv2/',
                                                'gkg.csv.zip')


@dataclass
class GDELTMetaData:
    split_char : str
    col_descriptor : List[str]
    col_descriptor_ref_url : str

gdelt_meta = GDELTMetaData(split_char='\\t',
                           col_descriptor=[\
                               'Date',
                               'SourceCollectionID',
                               'SourceCommonName',
                               'DocumentIdentifier',
                               'Counts_v1',
                               'Counts_v2',
                               'Themes',
                               'EnhancedThemes',
                               'Locations',
                               'EnhancedLocations',
                               'Persons',
                               'EnhancedPersons',
                               'Organizations',
                               'EnhancedOrganizations',
                               '5Tone',
                               'EnhancedDates',
                               'GCAM',
                               'SharingImage',
                               'RelatedImages',
                               'SocialImageEmbeds',
                               'SocialVideoEmbeds',
                               'Quotations',
                               'AllNames',
                               'Amounts',
                               'TranslateInfo'
                           ],
                           col_descriptor_ref_url='http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf',
                           )


class GDELTAccessor(object):
    '''Bla bla

    '''
    def __init__(self, line_filter_func, accessor_consts,
                 metadata,
                 dt_start, dt_end=None, tz_slack=1,
                 line_filter_func_kwargs={}):

        self.line_filter_func = line_filter_func
        self.line_filter_func_kwargs = line_filter_func_kwargs

        self.accessor_consts = accessor_consts
        self.metadata = metadata

        self.dt_start = dt_start
        if dt_end is None:
            self.dt_end = datetime.today() - timedelta(hours=tz_slack)
        else:
            self.dt_end = dt_end
        self.delta_increase = timedelta(minutes=self.accessor_consts.minute_increment)

        self.dt_iter = self._make_dt_iter()

    def _make_dt_iter(self):
        dts = [self.dt_start]
        dt_current = self.dt_start
        while dt_current < self.dt_end:
            dt_current += self.delta_increase
            dts.append(dt_current)

        return dts

    def __iter__(self):
        return self

    def __next__(self):
        try:
            dt_current = self.dt_iter.pop(0)
            return self._get_df_selection(self.accessor_consts.make_url(dt_current))
        except IndexError:
            raise StopIteration

    def _get_df_selection(self, url_str):
        url = urllib.request.urlopen(url_str)
        raw_data = {}
        with ZipFile(BytesIO(url.read())) as my_zip_file:
            for contained_file in my_zip_file.namelist():
                for line in my_zip_file.open(contained_file).readlines():
                    line_str = str(line)
                    if self.line_filter_func(line_str, **self.line_filter_func_kwargs):
                        dd = line_str.split(self.metadata.split_char)
                        raw_data[dd[0][2:]] = dd[1:-1]

        return self.make_a_panda(raw_data)

    def make_a_panda(self, dict_data):
        if not all([len(x) == len(self.metadata.col_descriptor) for x in dict_data.values()]):
            raise RuntimeError('Dictionary values not all of length {}'.format(len(self.metadata.col_descriptor)))
        return pd.DataFrame.from_dict(dict_data, orient='index', columns=self.metadata.col_descriptor)


class GDELTParser(object):
    '''Bla bla

    '''
    def __init__(self):
        pass

    def themes(self):
        pass