import os
import sys
import re
import json
import time
import datetime
import gzip

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

import logging
from pythonjsonlogger import jsonlogger

lib_path = os.path.abspath(os.path.join('..','lib'))
sys.path.append(lib_path)
import redshift_utils as rs
import s3_utils as s3

# Setup logging
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
logHandler = logging.StreamHandler()
logHandler.setFormatter(formatter)
logger = logging.getLogger('events_logger')
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)


def _read_event_text(json_path = os.path.join('..','json_and_txt'), **kwargs):

    with open(os.path.join(json_path, 'mktg_events.json'), 'r') as f:
        mktg_events = json.load(f)

    with open(os.path.join(json_path, 'event_dates_select.txt'), 'r') as f:
        event_dates_str = f.read()

    return mktg_events, event_dates_str


def sample_downloaded_data(filename, sample_size=250000, sample_seed=None, group_col='persona', target_col='target_shopped_ind', **kwargs):
    start = time.time()
    logger.info('Sampling data from downloaded file...')

    if sample_seed is not None:
        np.random.seed(sample_seed)

    group_ord = []
    group_num = []
    target_num = []

    with gzip.open(filename, 'rb') as f:
        header = f.readline().strip().split('|')
        gx = header.index(group_col)
        tx = header.index(target_col)
        mx = max(gx,tx)
        for row in f:
            row = row.split('|',mx+1)
            grp = row[gx]
            tgt = row[tx]
            if len(group_ord) == 0 or grp != group_ord[-1]:
                group_ord.append(grp)
                group_num.append(0)
                target_num.append(0)
            group_num[-1] += 1
            target_num[-1] += int(tgt)
        group_size = sample_size / len(group_ord)
        skip = np.concatenate(map(lambda n,s: s - np.random.choice(n, n-group_size, replace=False),
                                  group_num, np.cumsum(group_num)))
        skip.sort()
        f.seek(0)
        sample = pd.read_csv(f, header=0, delimiter='|', skiprows=skip)

    end = time.time()
    logger.info('Sampling data required {}s'.format(round(end - start, 3)))

    summary = pd.DataFrame({'n': group_num, 'n_pos': target_num},
                           index=pd.Index(group_ord, name='persona')).sort_index()

    return sample, summary


def scale_data(df_train, df_test, features):
    nonfeatures = [col for col in df_train.columns if col not in features]
    ldf_train = pd.DataFrame(df_train[nonfeatures].values, columns=nonfeatures)
    ldf_test = pd.DataFrame(df_test[nonfeatures].values, columns=nonfeatures)

    SS = StandardScaler(with_mean=True, with_std=True)
    SS.fit(df_train[features])
    tdf_train = pd.DataFrame(SS.transform(df_train[features]), columns=features)
    tdf_test = pd.DataFrame(SS.transform(df_test[features]), columns=features)

    sdf_train = pd.concat([ldf_train, tdf_train], axis=1)
    sdf_test = pd.concat([ldf_test, tdf_test], axis=1)

    return sdf_train, sdf_test


def main(event, year, sql_path=os.path.join('..','sql'), test=False, **kwargs):

    suffix = '_test' if test else ''

    mktg_events, event_dates_str = _read_event_text(**kwargs)

    event_dates_txt = "\nunion\n\n".join(map(lambda s: event_dates_str.format(suffix, **s), mktg_events))

    event_dict = filter(lambda d: d['event']==event, mktg_events)[0]
    short_event = event_dict['short_event']

    date_tag = time.strftime('%Y%m%d') if 'date_tag' not in kwargs else kwargs['date_tag']
    out_handle = 'ep_{0}_{1}_{2}_'.format(short_event, year, date_tag)
    out_filename = out_handle + '000.gz'

    slug = event_dict['event_slug'] + str(year % 1000)

    dl_path = '.' if 'dl_path' not in kwargs else kwargs['dl_path']
    data_path = dl_path if 'data_path' not in kwargs else kwargs['data_path']

    if 'skip_data_pull' not in kwargs or not kwargs['skip_data_pull']:
        environment = kwargs['environment']
        s3_bucket = kwargs['s3_bucket']
        s3_path = kwargs['s3_path']

        start = time.time()
        logger.info('Getting target event date span...')
        sql = rs.read_sql_file(os.path.join(sql_path, '00_get_event_span.sql'))
        sql = sql.format(suffix, event, year)
        rows, header = rs.execute_rs_query(sql, return_data=True)
        end = time.time()
        logger.info('Obtaining target event dates required {}s'.format(round(end - start, 3)))

        target_start_dt, target_end_dt = rows[0]

        if 'feature_date_offset' in kwargs:
            feature_end_dt = target_start_dt - datetime.timedelta(days=kwargs['feature_date_offset'])
        else:
            feature_end_dt = target_start_dt

        start = time.time()
        logger.info('Creating and unloading features...')
        logger.info('This might take some time')
        creds = s3.get_temp_creds(environment=environment, profile_name='default')
        sql = rs.read_sql_file(os.path.join(sql_path, '01_unload_data.sql'))
        sql = sql.format(suffix, target_start_dt, target_end_dt, feature_end_dt,
                         event_dates_txt, s3_bucket, s3_path, out_handle, creds)
        rs.execute_rs_query(sql)
        end = time.time()
        logger.info('Creating and unloading features required {}s'.format(round(end - start, 3)))

        start = time.time()
        logger.info('Downloading features locally...')
        s3.download_file_from_s3(s3_bucket, s3_path, out_filename, filepath=dl_path,
                                 environment=environment, profile_name='default')
        end = time.time()
        logger.info('Downloading features required {}s'.format(round(end - start, 3)))

    sample_df, summary_df = sample_downloaded_data(os.path.join(dl_path, out_filename), **kwargs)

    if 'save_summary' in kwargs and kwargs['save_summary']:
        summary_file = 'ep_{0}_{1}_summary.csv'
        summary_df.to_csv(os.path.join(data_path, summary_file.format(slug, 'data')), index=True)
        sample_gb = sample_df.groupby('persona')['target_shopped_ind'].agg(['count','sum'])
        sample_gb = sample_gb.rename(columns={'count': 'n', 'sum': 'n_pos'})
        sample_gb.to_csv(os.path.join(data_path, summary_file.format(slug, 'sample')), index=True)

    if 'log_features' in kwargs:
        log_fn = np.log if 'log_fn' not in kwargs else kwargs['log_fn']
        for col in kwargs['log_features']:
            if col in sample_df.columns:
                sample_df[col] = log_fn(sample_df[col])
                sample_df.rename(columns={col: 'log_'+col}, inplace=True)

    start = time.time()
    logger.info('Splitting, scaling, and saving sample data...')
    features = [col for col in sample_df.columns if col not in ['cust_key','persona','target_shopped_ind']]
    split_state = None if 'split_state' not in kwargs else kwargs['split_state']
    train_size = None if 'train_size' not in kwargs else kwargs['train_size']
    test_size = None if 'test_size' not in kwargs else kwargs['test_size']
    stratify_col = 10*sample_df['persona'] + sample_df['target_shopped_ind']
    sample_df_train, sample_df_test = train_test_split(sample_df, train_size=train_size, test_size=test_size,
                                                       random_state=split_state, stratify=stratify_col)
    scaled_df_train, scaled_df_test = scale_data(sample_df_train, sample_df_test, features)
    data_file = 'ep_{0}_{1}_sample.csv.gz'
    sample_df_train.to_csv(os.path.join(data_path, data_file.format(slug, 'unscaled_train')),
                           index=False, sep='|', compression='gzip')
    sample_df_test.to_csv(os.path.join(data_path, data_file.format(slug, 'unscaled_test')),
                          index=False, sep='|', compression='gzip')
    scaled_df_train.to_csv(os.path.join(data_path, data_file.format(slug, 'scaled_train')),
                           index=False, sep='|', compression='gzip')
    scaled_df_test.to_csv(os.path.join(data_path, data_file.format(slug, 'scaled_test')),
                          index=False, sep='|', compression='gzip')
    end = time.time()
    logger.info('Splitting and scaling sample required {}s'.format(round(end - start, 3)))

    ## Pick up from here



def logm1(x):
    y = x.copy()
    y[y <= 0] = -1
    y[y > 0] = np.log(y[y > 0])
    return y


if __name__ == '__main__':

    #event_year_pairs = [{"event": "mktg_valentines_day", "year": 2017}],
    #                    {"event": "mktg_valentines_day", "year": 2018}]
    event_year_pairs = [{"event": "anniversary_public_event", "year": 2017}]

    params = {
        "test": False,
        "sql_path": os.path.join('..','sql'),
        "dl_path": os.path.join('temp','downloads'),
        "json_path": os.path.join('..','json_and_txt'),
        "data_path": os.path.join('..','data'),
        "environment": 'local',
        "s3_bucket": 'liveramp-testing',
        "s3_path": 'event_propensity/temp_data/',
        "feature_date_offset": 30,
        "skip_data_pull": True,
        "date_tag": "20180417",
        "sample_size": 250000,
        "sample_seed": 30132,
        "save_summary": True,
        "log_features": ['fl_total_spend','fl_total_trips','fl_avg_spend_per_trip',
                         'fl_total_spend_ly', 'fl_total_trips_ly','fl_avg_spend_per_trip_ly'],
        "log_fn": logm1, #lambda x: np.log1p(x) if (x <= 0).any() else np.log(x)
        #"transform_dict" : {'tenure_total_months': lambda x: np.fmin(x/96, 1),
        #                    'months_since_last_sale': lambda x: x/24},
        "split_state": 8379,
        "train_size": 0.7
    }

    for eyp in event_year_pairs:
        main(eyp['event'], eyp['year'], **params)
