# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
import numpy as np
import pandas as pd
from rqdatac.services.calendar import is_trading_date, get_previous_trading_date

FIELD_METHOD_MAP = {
    "open": "first",
    "close": "last",
    "iopv": "last",
    "high": np.maximum,
    "low": np.minimum,
    "limit_up": np.maximum,
    "limit_down": np.minimum,
    "total_turnover": np.add,
    "volume": np.add,
    "num_trades": np.add,
    "acc_net_value": "last",
    "unit_net_value": "last",
    "discount_rate": "last",
    "settlement": "last",
    "prev_settlement": "last",
    "open_interest": "last",
    "basis_spread": "last",
    "date": "last",
    "trading_date": "last",
    "datetime": "last",
}
FIELD_METHOD_MAP2 = {
    "open": "first",
    "close": "last",
    "iopv": "last",
    "high": "max",
    "low": "min",
    "total_turnover": "sum",
    "volume": "sum",
    "num_trades": "sum",
    "acc_net_value": "last",
    "unit_net_value": "last",
    "discount_rate": "last",
    "settlement": "last",
    "prev_settlement": "last",
    "open_interest": "last",
    "basis_spread": "last",
    "contract_multiplier": "last",
    "strike_price": "last",
}

def resample_day_bar(array, n, field):
    length = len(array)
    how = FIELD_METHOD_MAP[field]
    if how == 'first':
        return array[::n]

    # python2.7 `/` equals '//'
    slices = np.arange(0, int(np.ceil(length / float(n))) * n + 1, n)
    slices[-1] = length
    if how == 'last':
        return array[slices[1:] - 1]

    return how.reduceat(array, slices[:-1])

def _update_weekly_trading_date_index(idx):
    if is_trading_date(idx[1]):
        return idx
    return (idx[0], get_previous_trading_date(idx[1]))

def resample_week_df(df, fields):
    hows = {field:FIELD_METHOD_MAP2[field] for field in fields}
    res1 = df[df['volume'] > 0].groupby('order_book_id').resample('W-Fri', level=1).agg(hows)
    res2 = df[df['volume'] == 0].groupby('order_book_id').resample('W-Fri', level=1).agg(hows)
    res = pd.concat([res1, res2])
    res = res[~res.index.duplicated(keep='first')]
    res.index = res.index.map(_update_weekly_trading_date_index)
    res.sort_index(inplace=True)
    return res
