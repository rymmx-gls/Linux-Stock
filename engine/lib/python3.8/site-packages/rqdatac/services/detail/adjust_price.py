# -*- coding: utf-8 -*-
#
# Copyright 2018 Ricequant, Inc
import time
from threading import Lock

import numpy as np
import pandas as pd

from rqdatac.client import get_client

_lock = Lock()
_expired = time.time()
_ex_factor_cache = {}
_split_factor_cache = {}

MIN_DATE = pd.Timestamp(1800, 1, 1)


def get_ex_factor_for(order_book_ids, market):
    with _lock:
        _prepare_ex_factor_for(order_book_ids, market)
        return {ob: _ex_factor_cache[ob] for ob in order_book_ids}


def _prepare_ex_factor_for(order_book_ids, market):
    global _ex_factor_cache, _expired, _split_factor_cache
    if _expired < time.time():
        _ex_factor_cache.clear()
        _split_factor_cache.clear()
        _expired = time.time() + 3600 * 8

    missing = [ob for ob in order_book_ids if ob not in _ex_factor_cache]
    if not missing:
        return

    data = get_client().execute('_get_ex_factor', missing, market)
    if not data:
        _ex_factor_cache.update((ob, None) for ob in missing)
        return

    remain = set(r['order_book_id'] for r in data)
    # for each order_book_id, we add an init ex_factor value
    data.extend({'order_book_id': o, 'ex_date': MIN_DATE, 'ex_cum_factor': 1.0} for o in remain)

    df = pd.DataFrame(data)
    df = df[df.ex_date <= pd.datetime.today()]
    df.set_index('ex_date', inplace=True)
    df.sort_index(inplace=True)

    for order_book_id, s in df.groupby('order_book_id')['ex_cum_factor']:
        _ex_factor_cache[order_book_id] = s

    for order_book_id in missing:
        if order_book_id not in _ex_factor_cache:
            _ex_factor_cache[order_book_id] = None


def get_split_factor_for(order_book_ids, market):
    with _lock:
        _prepare_split_for(order_book_ids, market)
        return {ob: _split_factor_cache[ob] for ob in order_book_ids}


def _prepare_split_for(order_book_ids, market):
    global _ex_factor_cache, _expired, _split_factor_cache
    if _expired < time.time():
        _ex_factor_cache.clear()
        _split_factor_cache.clear()
        _expired = time.time() + 3600 * 8

    missing = [ob for ob in order_book_ids if ob not in _split_factor_cache]
    if not missing:
        return

    data = get_client().execute('_get_split', missing, market)
    if not data:
        _split_factor_cache.update((ob, None) for ob in missing)
        return

    remain = set(r['order_book_id'] for r in data)
    # for each order_book_id, we add an init split value
    data.extend(
        {
            'order_book_id': ob,
            'split_coefficient_to': 1.0,
            'split_coefficient_from': 1.0,
            'cum_factor': 1.0,
            'ex_dividend_date': MIN_DATE,
        } for ob in remain
    )

    df = pd.DataFrame(data)
    df = df[df.ex_dividend_date <= pd.datetime.today()]
    df.set_index('ex_dividend_date', inplace=True)
    df.sort_index(inplace=True)
    df["cum_factor"] = df["split_coefficient_to"] / df["split_coefficient_from"]
    df["cum_factor"] = df.groupby("order_book_id")["cum_factor"].cumprod()
    for order_book_id, s in df.groupby('order_book_id')['cum_factor']:
        s = s[~s.index.duplicated(keep='last')]
        # split index 有可能重复
        _split_factor_cache[order_book_id] = s.copy()

    for order_book_id in missing:
        if order_book_id not in _split_factor_cache:
            _split_factor_cache[order_book_id] = None


PRICE_FIELDS = {"low", "close", "limit_up", "limit_down", "unit_net_value", "open", "high"}

FIELDS_NEED_TO_ADJUST = PRICE_FIELDS | {'volume'}


def adjust_price(panel, order_book_ids, how, market):
    r_map_fields = {f: i for i, f in enumerate(panel.items) if f in FIELDS_NEED_TO_ADJUST}
    if not r_map_fields:
        # 没有需要复权的字段
        return

    timestamps = panel.major_axis

    ex_factors = get_ex_factor_for(order_book_ids, market)
    split_factors = None
    if 'volume' in r_map_fields:
        split_factors = get_split_factor_for(order_book_ids, market)

    # 是否前复权
    pre = how == 'pre'

    # 三维，(field, timestamp, order_book_id)
    data = panel.values
    for i, order_book_id in enumerate(panel.minor_axis):
        if order_book_id not in order_book_ids:
            continue
        ex_factor = ex_factors[order_book_id]
        if ex_factor is None:
            # 如果 ex_factor 为空，则不存在拆分及分红，无需后续处理
            continue

        factor = np.take(ex_factor.values, ex_factor.index.searchsorted(timestamps, side='right') - 1)
        if pre:
            factor /= ex_factor.iloc[-1]

        split_factor = split_factors.get(order_book_id, None) if split_factors else None
        if split_factor is not None:
            tmp = 1.0 / np.take(split_factor.values, split_factor.index.searchsorted(timestamps, side='right') - 1)
            if pre:
                tmp *= split_factor.iloc[-1]
            split_factor = tmp

        for f, j in r_map_fields.items():
            if f in PRICE_FIELDS:
                data[j, :, i] *= factor
                np.round(data[j, :, i], 4, out=data[j, :, i])
            elif split_factor is not None:
                data[j, :, i] *= split_factor
                np.round(data[j, :, i], 0, out=data[j, :, i])


def adjust_price_multi_df(df, order_book_ids, how, obid_slice_map, market):
    r_map_fields = {f: i for i, f in enumerate(df.columns) if f in FIELDS_NEED_TO_ADJUST}
    if not r_map_fields:
        # 没有需要复权的字段
        return

    ex_factors = get_ex_factor_for(order_book_ids, market)
    split_factors = None
    if 'volume' in r_map_fields:
        split_factors = get_split_factor_for(order_book_ids, market)

    # 是否前复权
    pre = how == 'pre'

    data = df.values
    timestamps_level = df.index.get_level_values(1)
    for order_book_id, slice_ in obid_slice_map.items():
        if order_book_id not in order_book_ids:
            continue
        ex_factor = ex_factors[order_book_id]
        if ex_factor is None:
            # 如果 ex_factor 为空，则不存在拆分及分红，无需后续处理
            continue
        timestamps = timestamps_level[slice_]
        factor = np.take(ex_factor.values, ex_factor.index.searchsorted(
            timestamps, side='right') - 1)
        if pre:
            factor /= ex_factor.iloc[-1]

        split_factor = split_factors.get(order_book_id, None) if split_factors else None
        if split_factor is not None:
            tmp = 1.0 / np.take(split_factor.values,
                                split_factor.index.searchsorted(timestamps, side='right') - 1)
            if pre:
                tmp *= split_factor.iloc[-1]
            split_factor = tmp

        for f, j in r_map_fields.items():
            if f in PRICE_FIELDS:
                data[slice_, j] *= factor
                np.round(data[slice_, j], 4, out=data[slice_, j])
            elif split_factor is not None:
                data[slice_, j] *= split_factor
                np.round(data[slice_, j], 0, out=data[slice_, j])
