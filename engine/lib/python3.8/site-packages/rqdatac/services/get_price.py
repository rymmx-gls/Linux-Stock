# -*- coding: utf-8 -*-
import datetime
import warnings
import pickle

import pandas as pd
import numpy as np

from rqdatac.services.basic import instruments
from rqdatac.services.calendar import get_next_trading_date, is_trading_date, get_previous_trading_date
from rqdatac.services.future import get_dominant_future
from rqdatac.services.stock_status import is_suspended
from rqdatac.services.live import get_ticks
from rqdatac.services.detail.resample_helper import resample_week_df

from rqdatac.validators import (
    ensure_string,
    ensure_list_of_string,
    check_items_in_container,
    ensure_instruments,
    ensure_date_range,
    is_panel_removed,
    raise_for_no_panel,
)
from rqdatac.utils import (
    to_date_int,
    to_datetime,
    to_date,
    int8_to_datetime,
    int14_to_datetime,
    int14_to_datetime_v,
    int17_to_datetime_v,
    int17_to_datetime,
    today_int,
    date_to_int8,
    pf_fill_nan,
    string_types
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, ttl_cache, compatible_with_parm
from rqdatac.share.errors import PermissionDenied, MarketNotSupportError


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_price(
    order_book_ids,
    start_date=None,
    end_date=None,
    frequency="1d",
    fields=None,
    adjust_type="pre",
    skip_suspended=False,
    expect_df=False,
    market="cn",
    **kwargs
):
    """获取证券的历史数据

    :param order_book_ids: 股票列表
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :param start_date: 开始日期, 如 '2013-01-04' (Default value = None)
    :param end_date: 结束日期, 如 '2014-01-04' (Default value = None)
    :param frequency: 可选参数, 默认为日线。日线使用 '1d', 分钟线 '1m' (Default value = "1d")
    :param fields: 可选参数。默认为所有字段。 (Default value = None)
    :param adjust_type: 可选参数, 默认为'pre', 返回前复权数据。设置为'none'将返回原始数据,
        'post'返回后复权数据,
        'internal'返回只包含拆分的前复权数据。 (Default value = "pre")
    :param skip_suspended: 可选参数，默认为False；当设置为True时，返回的数据会过滤掉停牌期间，
                    此时order_book_ids只能设置为一只股票 (Default value = False)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :returns: 如果仅传入一只股票, 返回一个 pandas.DataFrame
        如果传入多只股票, 则返回一个 pandas.Panel

    """
    if frequency == "tick":
        return get_tick_price(order_book_ids, start_date, end_date, fields, expect_df, market)
    elif frequency.endswith(("d", "m", "w")):
        duration = int(frequency[:-1])
        frequency = frequency[-1]
        assert 1 <= duration <= 240, "frequency should in range [1, 240]"
        if market == "hk" and frequency == "m" and duration not in (1, 5, 15, 30, 60):
            raise ValueError("frequency should be str like 1m, 5m, 15m 30m,or 60m")
        elif frequency == 'w' and duration not in (1,):
            raise ValueError("Weekly frequency should be str '1w'")
    else:
        raise ValueError("frequency should be str like 1d, 1m, 5m or tick")
    if "adjusted" in kwargs:
        adjusted = kwargs.pop("adjusted")
        adjust_type = "pre" if adjusted else "none"

    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))

    valid_adjust = ["pre", "post", "none"]
    ensure_string(adjust_type, "adjust_type")
    check_items_in_container(adjust_type, valid_adjust, "adjust_type")
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    if skip_suspended and len(order_book_ids) > 1:
        raise ValueError("only accept one order_book_id or symbol if skip_suspended is True")

    assert isinstance(skip_suspended, bool), "'skip_suspended' should be a bool"
    assert isinstance(expect_df, bool), "'expect_df' should be a bool"

    order_book_ids, stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos = classify_order_book_ids(
        order_book_ids, market
    )
    if not order_book_ids:
        warnings.warn("no valid instrument")
        return
    start_date, end_date = _ensure_date(
        start_date, end_date, stocks, funds, indexes, futures, spots, options, convertibles, repos
    )

    if not expect_df and not is_panel_removed and frequency != "w":

        if frequency == "d":
            fields, has_dominant_id = _ensure_fields(fields, DAYBAR_FIELDS, stocks, funds, futures, spots, options, convertibles, indexes, repos)
            pf = get_daybar(order_book_ids, start_date, end_date, fields, duration, market)
            if pf is None:
                return
        else:
            fields, has_dominant_id = _ensure_fields(fields, MINBAR_FIELDS, stocks, funds, futures, spots, options, convertibles, indexes, repos)
            history_permission_denied, today_permission_denied = False, False
            try:
                pf = get_minbar(order_book_ids, start_date, end_date, fields, duration, market)
            except (PermissionDenied, MarketNotSupportError):
                pf = None
                history_permission_denied = True
            history_latest_day = 0 if pf is None else date_to_int8(pf.iloc[-1].index[-1])
            if history_latest_day < end_date and end_date >= today_int():
                try:
                    today_pf = get_today_minbar(order_book_ids, fields, duration, market)
                except (PermissionDenied, MarketNotSupportError):
                    today_pf = None
                    today_permission_denied = True
                if today_pf is None:
                    today_pf_latest_day = 0
                else:
                    today_pf_latest_day = date_to_int8(get_current_trading_date(today_pf.iloc[-1].index[-1]))
                if today_pf_latest_day > history_latest_day and today_pf_latest_day >= start_date:
                    if history_latest_day == 0:
                        pf = today_pf
                    else:
                        pf = pd.concat([pf, today_pf], axis=1)
            if pf is None:
                if history_permission_denied and today_permission_denied:
                    raise PermissionDenied("Not permit to get minbar price ")
                elif history_permission_denied:
                    warnings.warn("Not permit to get history minbar price")
                elif today_permission_denied:
                    warnings.warn("Not permit to get realtime minbar price")
                return

        result = _adjust_pf(
            pf,
            order_book_ids,
            stocks,
            funds,
            futures888,
            start_date,
            end_date,
            frequency,
            fields,
            has_dominant_id,
            adjust_type,
            skip_suspended,
            market,
        )
        return result
    elif frequency == 'w':
        if expect_df == False:
            raise ValueError("Weekly frequency can only return a DataFrame object, set 'expect_df' to True to resolve this")
        if skip_suspended == True:
            raise ValueError("Weekly frequency does not support skipping suspended trading days, set 'skip_suspended' to False to resolve this")
        start_date, end_date = _weekly_start_end_date_handler(start_date, end_date)
        if start_date > end_date:
            # 如果*当周没有结束*
            # 或者start date 和 end date 不能涵盖当周所有的交易日，查询该周的数据时返回为空。
            return None

        fields, has_dominant_id = _ensure_fields(fields, WEEKBAR_FIELDS, stocks, funds, futures, spots, options, convertibles, indexes, repos)
        return get_week_df(order_book_ids, start_date, end_date, fields, adjust_type, market, *(stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos))
    else:
        from rqdatac.services.detail.get_price_df import get_price_df
        df = get_price_df(
            order_book_ids, start_date, end_date, frequency, duration, fields, adjust_type, skip_suspended,
            stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos, market
        )
        if df is None or expect_df:
            return df

        if len(df.index.get_level_values(0).unique()) == 1:
            df.reset_index(level=0, inplace=True, drop=True)
            # df.index.name = None
            if len(df.columns) == 1:
                df = df[df.columns[0]]
            return df
        elif len(df.columns) == 1:
            field = df.columns[0]
            df = df.unstack(0)[field]
            # df.index.name = None
            df.columns.name = None
            return df
        raise_for_no_panel(False)


ZERO_FILL_FIELDS = frozenset({"total_turnover", "open_interest", "volume"})

DAYBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest", "limit_up", "limit_down"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["limit_up", "limit_down", "num_trades"],
    "fund": ["limit_up", "limit_down", "num_trades", "iopv"],
    "spot": ["open_interest"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "prev_settlement", "settlement", "limit_up", "limit_down"],
    "convertible": ["num_trades"],
    "index": ["num_trades"],
    "repo": ["num_trades"],
}

WEEKBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["num_trades"],
    "fund": ["num_trades", "iopv"],
    "spot": ["open_interest"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "settlement"],
    "convertible": ["num_trades"],
    "index": ["num_trades"],
    "repo": ["num_trades"],
}

MINBAR_FIELDS = {
    "future": ["trading_date", "open_interest"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": [],
    "fund": ["iopv"],
    "spot": [],
    "option": ["trading_date", "open_interest"],
    "convertible": [],
    "index": [],
    "repo": [],
}

SPOT_DIRECTION_MAP = {0: "null", 1: "多支付空", 2: "空支付多", 3: "交收平衡"}


def _ensure_date(start_date, end_date, stocks, funds, indexes, futures, spots, options, convertibles, repos):
    default_start_date, default_end_date = ensure_date_range(start_date, end_date)

    only_futures = futures and (not stocks) and (not funds) and (not indexes) and (not spots) and (
        not options) and (not convertibles) and (not repos)
    if only_futures and len(futures) == 1:
        # 如果只有一只期货, 则给 start_date 和 end_date 合适的默认值
        # 连续合约的listed_date和de_listed_date都为0, 因此需要特殊处理
        if futures[0].listed_date != "0000-00-00":
            default_start_date = to_date_int(futures[0].listed_date)
        if futures[0].de_listed_date != "0000-00-00":
            default_end_date = to_date_int(futures[0].de_listed_date)

    start_date = to_date_int(start_date) if start_date else default_start_date
    end_date = to_date_int(end_date) if end_date else default_end_date
    if start_date < 20000104:
        warnings.warn("start_date is earlier than 2000-01-04, adjusted to 2000-01-04")
        start_date = 20000104
    return start_date, end_date


def _ensure_fields(fields, fields_dict, stocks, funds, futures, spots, options, convertibles, indexes, repos):
    has_dominant_id = True
    all_fields = set(fields_dict["common"])
    if futures:
        all_fields.update(fields_dict["future"])
    if stocks:
        all_fields.update(fields_dict["stock"])
    if funds:
        all_fields.update(fields_dict["fund"])
    if spots:
        all_fields.update(fields_dict["spot"])
    if options:
        all_fields.update(fields_dict["option"])
    if convertibles:
        all_fields.update(fields_dict["convertible"])
    if indexes:
        all_fields.update(fields_dict["index"])
    if repos:
        all_fields.update(fields_dict["repo"])
    if fields:
        fields = ensure_list_of_string(fields, "fields")
        fields_set = set(fields)
        if len(fields_set) < len(fields):
            warnings.warn("duplicated fields: %s" % [f for f in fields if fields.count(f) > 1])
            fields = list(fields_set)
        if 'dominant_id' in fields:
            if len(fields) == 1:
                raise ValueError("can't get dominant_id separately, please use futures.get_dominant")
            fields.remove('dominant_id')
        else:
            has_dominant_id = False
        check_items_in_container(fields, all_fields, "fields")
        return fields, has_dominant_id
    else:
        return list(all_fields), has_dominant_id


def classify_order_book_ids(order_book_ids, market):
    ins_list = ensure_instruments(order_book_ids, market=market)
    _order_book_ids = []
    stocks = []
    funds = []
    indexes = []
    futures = []
    futures_888 = {}
    spots = []
    options = []
    convertibles = []
    repos = []
    for ins in ins_list:
        if ins.order_book_id not in _order_book_ids:
            _order_book_ids.append(ins.order_book_id)
            if ins.type == "CS":
                stocks.append(ins.order_book_id)
            elif ins.type == "INDX":
                indexes.append(ins.order_book_id)
            elif ins.type in {"ETF", "LOF", "SF"}:
                funds.append(ins.order_book_id)
            elif ins.type == "Future":
                if ins.order_book_id.endswith(("888", "889")):
                    futures_888[ins.order_book_id] = ins.underlying_symbol
                futures.append(ins)
            elif ins.type == "Spot":
                spots.append(ins.order_book_id)
            elif ins.type == "Option":
                options.append(ins.order_book_id)
            elif ins.type == "Convertible":
                convertibles.append(ins.order_book_id)
            elif ins.type == "Repo":
                repos.append(ins.order_book_id)
    return _order_book_ids, stocks, funds, indexes, futures, futures_888, spots, options, convertibles, repos

def _weekly_start_end_date_handler(start_date, end_date):
    start_date = to_date(start_date)
    monday = start_date - datetime.timedelta(days=start_date.weekday())
    first_trading_day_in_week = monday if is_trading_date(monday) else get_next_trading_date(monday)
    if first_trading_day_in_week < start_date:
        start_date = monday + datetime.timedelta(weeks=1)

    end_date = to_date(end_date)
    if end_date > datetime.date.today():
        end_date = datetime.date.today()
    friday = end_date - datetime.timedelta(days=end_date.weekday()) + datetime.timedelta(days=4)
    last_trading_day_in_week = friday if is_trading_date(friday) else get_previous_trading_date(friday)
    if last_trading_day_in_week > end_date:
        end_date = friday - datetime.timedelta(weeks=1)

    return to_date_int(start_date), to_date_int(end_date)

def get_week_df(order_book_ids, start_date, end_date, fields, adjust_type, market, stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos):
    res = []
    has_volume_field = 'volume' in fields
    if not has_volume_field:
        fields.append('volume')
    from rqdatac.services.detail.get_price_df import get_price_df
    df = get_price_df(
            order_book_ids, start_date, end_date, 'd', 1, fields, adjust_type, False,
            stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos, market
        )
    res = resample_week_df(df, fields)
    if not has_volume_field:
        res.drop(columns=['volume'], inplace=True)
    return res

def get_daybar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_daybar_v", order_book_ids, start_date, end_date, fields, 1, market=market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    trading_dates = set()
    for _, d in data:
        trading_dates.update(d['date'])
    if not trading_dates:
        return
    sorted_trading_dates = sorted(trading_dates)
    trading_dates = np.array(sorted_trading_dates)

    arr = np.full((len(fields), len(trading_dates), len(order_book_ids)), np.nan)
    r_map_fields = {f: i for i, f in enumerate(fields)}
    r_map_order_book_ids = {o: i for i, o in enumerate(order_book_ids)}
    for f in ZERO_FILL_FIELDS:
        if f in fields:
            arr[r_map_fields[f], :, :] = 0

    for obid, d in data:
        dates = d['date']
        if len(dates) == 0:
            continue

        idx = trading_dates.searchsorted(dates, side='left')
        for f, value in d.items():
            if f == 'date':
                continue
            arr[r_map_fields[f], idx, r_map_order_book_ids[obid]] = value

    if duration != 1:
        from .detail.resample_helper import resample_day_bar
        trading_dates = resample_day_bar(trading_dates, duration, 'date')
        resampled = np.full((len(fields), len(trading_dates), len(order_book_ids)), np.nan)
        for f in fields:
            resampled[r_map_fields[f], :, :] = resample_day_bar(arr[r_map_fields[f], :, :], duration, f)
        arr = resampled

    trading_dates = pd.to_datetime([int8_to_datetime(d) for d in trading_dates])
    return pd.Panel(data=arr, items=fields, major_axis=trading_dates, minor_axis=order_book_ids)

def get_minbar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_minbar_v", order_book_ids, start_date, end_date, fields, duration, market=market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    timestamps = set()
    for _, d in data:
        timestamps.update(d['datetime'])
    if not timestamps:
        return
    timestamps = np.array(sorted(timestamps))
    arr = np.full((len(fields), len(timestamps), len(order_book_ids)), np.nan)
    r_map_fields = {f: i for i, f in enumerate(fields)}
    r_map_order_book_ids = {o: i for i, o in enumerate(order_book_ids)}
    for f in ZERO_FILL_FIELDS:
        if f in fields:
            arr[r_map_fields[f], :, :] = 0

    for obid, d in data:
        dts = d['datetime']
        if len(dts) == 0:
            continue

        idx = timestamps.searchsorted(dts, side='left')
        for f, value in d.items():
            if f == 'datetime':
                continue
            arr[r_map_fields[f], idx, r_map_order_book_ids[obid]] = value

    timestamps = pd.to_datetime(int14_to_datetime_v(timestamps))
    return pd.Panel(data=arr, items=fields, major_axis=timestamps, minor_axis=order_book_ids)


def get_today_minbar(order_book_ids, fields, duration, market="cn"):
    data = get_client().execute("get_today_minbar", order_book_ids, fields, duration, market=market)
    columns = fields + ["datetime"]
    ret = {}
    for obid, d in data:
        if not d["datetime"]:
            continue
        df = (
            pd.DataFrame(d, columns=columns)
            .astype({i: "f8" for i in fields})
            .fillna({i: 0 for i in fields if i in ZERO_FILL_FIELDS})
        )
        df["datetime"] = df["datetime"].map(int14_to_datetime, na_action="ignore")
        df.set_index("datetime", inplace=True)
        ret[obid] = df
    if not ret:
        return
    pf = pd.Panel.from_dict(ret, orient="minor").reindex(items=fields)
    return pf_fill_nan(pf, order_book_ids)


@ttl_cache(15 * 60)
def daybar_for_tick_price(order_book_id):
    ins = instruments(order_book_id)
    today = to_date_int(datetime.datetime.today())

    if ins.type == "Future":
        fields = ["prev_settlement", "open", "close", "limit_up", "limit_down"]
    elif ins.type == "Option":
        fields = ["prev_settlement", "open", "close"]
    elif ins.type in ("LOF", "ETF"):
        fields = ["open", "close", "limit_up", "limit_down", "iopv"]
    elif ins.type == "CS":
        fields = ["open", "close", "limit_up", "limit_down"]
    else:
        fields = ["open", "close"]

    df = get_price(
        ins.order_book_id,
        "2004-12-31",
        today,
        frequency="1d",
        fields=fields,
        adjust_type="none",
        skip_suspended=False,
        expect_df=is_panel_removed,
        market="cn",
    )

    if df is not None and isinstance(df.index, pd.MultiIndex):
        df.reset_index(level=0, inplace=True)
    return df


EQUITIES_TICK_FIELDS = [
    "trading_date", "open", "last", "high", "low",
    "prev_close", "volume", "total_turnover", "limit_up", "limit_down",
    "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "b5", "a1_v", "a2_v", "a3_v",
    "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v", "change_rate",
]
FUND_TICK_FIELDS = EQUITIES_TICK_FIELDS + ["iopv", "prev_iopv"]
FUTURE_TICK_FIELDS = EQUITIES_TICK_FIELDS + ["open_interest", "prev_settlement"]
EQUITIES_TICK_COLUMNS = EQUITIES_TICK_FIELDS
FUTURE_TICK_COLUMNS = [
    "trading_date", "open", "last", "high", "low", "prev_settlement",
    "prev_close", "volume", "open_interest", "total_turnover", "limit_up", "limit_down",
    "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "b5", "a1_v", "a2_v", "a3_v",
    "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v", "change_rate",
]
FUND_TICK_COLUMNS = FUND_TICK_FIELDS
RELATED_DABAR_FIELDS = {"open", "prev_settlement", "prev_close", "limit_up", "limit_down", "change_rate"}


def get_tick_price(order_book_ids, start_date, end_date, fields, expect_df, market):
    df = get_tick_price_multi_df(order_book_ids, start_date, end_date, fields, market)
    if df is not None and not expect_df and isinstance(order_book_ids, string_types):
        df.reset_index(level=0, drop=True, inplace=True)
    return df


def convert_history_tick_to_multi_df(data, dt_name, fields, convert_dt):
    line_no = 0
    dt_set = set()
    obid_level = []
    obid_slice_map = {}
    for i, (obid, d) in enumerate(data):
        dates = d.pop("date")
        if len(dates) == 0:
            continue
        times = d.pop("time")
        dts = d[dt_name] = [_convert_int_to_datetime(dt, tm) for dt, tm in zip(dates, times)]

        dts_len = len(dts)

        if not obid_level or obid_level[-1] != obid:
            obid_level.append(obid)
        obid_slice_map[(i, obid)] = slice(line_no, line_no + dts_len, None)

        dt_set.update(dts)
        line_no += dts_len

    if line_no == 0:
        return

    daybars = {}
    if set(fields) & RELATED_DABAR_FIELDS:
        ins_list = ensure_instruments(obid_level)
        for ins in ins_list:
            daybar = daybar_for_tick_price(ins.order_book_id)
            if daybar is not None:
                daybar['prev_close'] = daybar['close'].shift(1)
                if ins.type in ["ETF", "LOF"]:
                    daybar['prev_iopv'] = daybar['iopv'].shift(1)
            daybars[ins.order_book_id] = daybar
        fields_ = list(set(fields) | {"last", "volume"})
    else:
        fields_ = fields

    obid_idx_map = {o: i for i, o in enumerate(obid_level)}
    obid_label = np.empty(line_no, dtype=object)
    dt_label = np.empty(line_no, dtype=object)
    arr = np.full((line_no, len(fields_)), np.nan)
    r_map_fields = {f: i for i, f in enumerate(fields_)}

    dt_arr_sorted = np.array(sorted(dt_set))
    dt_level = convert_dt(dt_arr_sorted)

    for i, (obid, d) in enumerate(data):
        if dt_name not in d:
            continue
        dts = d[dt_name]
        slice_ = obid_slice_map[(i, obid)]
        for f, value in d.items():
            if f == dt_name:
                dt_label[slice_] = dt_arr_sorted.searchsorted(dts, side='left')
            else:
                arr[slice_, r_map_fields[f]] = value

        obid_label[slice_] = obid_idx_map[obid]

        trading_date = to_datetime(get_current_trading_date(int17_to_datetime(dts[-1])))
        if "trading_date" in r_map_fields:
            trading_date_int = date_to_int8(trading_date)
            arr[slice_, r_map_fields["trading_date"]] = trading_date_int

        daybar = daybars.get(obid)
        if daybar is not None:
            try:
                last = daybar.loc[trading_date]
            except KeyError:
                continue
            day_open = last["open"]
            if "open" in r_map_fields:
                arr[slice_, r_map_fields["open"]] = [day_open if v > 0 else 0.0 for v in d["volume"]]
            if "prev_close" in r_map_fields:
                arr[slice_, r_map_fields["prev_close"]] = last["prev_close"]
            if instruments(obid).type in ("ETF", "LOF"):
                if "prev_iopv" in r_map_fields:
                    arr[slice_, r_map_fields["prev_iopv"]] = last["prev_iopv"]

            if instruments(obid).type in ("CS", "ETF", "LOF", "Future"):
                if "limit_up" in r_map_fields:
                    arr[slice_, r_map_fields["limit_up"]] = last["limit_up"]
                if "limit_down" in r_map_fields:
                    arr[slice_, r_map_fields["limit_down"]] = last["limit_down"]

            if instruments(obid).type in ("Future", "Option"):
                if "prev_settlement" in r_map_fields:
                    arr[slice_, r_map_fields["prev_settlement"]] = last["prev_settlement"]
                if "change_rate" in r_map_fields:
                    arr[slice_, r_map_fields["change_rate"]] = arr[slice_, r_map_fields["last"]] / last[
                                                                                                "prev_settlement"] - 1
            elif "change_rate" in r_map_fields:
                arr[slice_, r_map_fields["change_rate"]] = arr[slice_, r_map_fields["last"]] / last["prev_close"] - 1

    try:
        func_is_singletz = getattr(pd._libs.lib, 'is_datetime_with_singletz_array')
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', lambda *args: True)
    except AttributeError:
        func_is_singletz = None
    multi_idx = pd.MultiIndex(
        [obid_level, dt_level],
        [obid_label, dt_label],
        names=('order_book_id', dt_name)
    )
    df = pd.DataFrame(data=arr, index=multi_idx, columns=fields_)
    if "trading_date" in r_map_fields:
        df["trading_date"] = df["trading_date"].astype(int).apply(int8_to_datetime)
    if func_is_singletz is not None:
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', func_is_singletz)
    return df[fields]


def get_history_tick(order_book_ids, start_date, end_date, gtw_fields, columns, market):
    data = get_client().execute("get_tickbar", order_book_ids, start_date, end_date, gtw_fields, market=market)
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    history_df = convert_history_tick_to_multi_df(data, "datetime", columns, int17_to_datetime_v)

    if isinstance(history_df, pd.DataFrame) and 'iopv' in history_df.columns:
        history_df['iopv'] = history_df['iopv'].replace(0.0, np.nan)
    return history_df


def get_tick_price_multi_df(order_book_ids, start_date, end_date, fields, market):
    ins_list = ensure_instruments(order_book_ids)
    order_book_ids = [ins.order_book_id for ins in ins_list]
    types = {ins.type for ins in ins_list}

    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=3))
    if "Future" in types or "Option" in types:
        base_fields = FUTURE_TICK_FIELDS
        base_columns = FUTURE_TICK_COLUMNS

    elif 'ETF' in types or 'LOF' in types:
        base_fields = FUND_TICK_FIELDS
        base_columns = FUND_TICK_COLUMNS

    else:
        base_fields = EQUITIES_TICK_FIELDS
        base_columns = EQUITIES_TICK_COLUMNS

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, base_fields, "fields")
        columns = [f for f in base_columns if f in fields]
    else:
        fields = base_fields
        columns = base_columns

    gtw_fields = set(fields) | {"date", "time"}
    if set(fields) & RELATED_DABAR_FIELDS:
        gtw_fields.update({"volume", "last"})

    history_df = get_history_tick(order_book_ids, start_date, end_date, list(gtw_fields), columns, market)
    history_latest_date = 0 if history_df is None else date_to_int8(get_current_trading_date(
                                                            history_df.index.get_level_values(1).max()))
    today = today_int()
    next_trading_date = date_to_int8(get_next_trading_date(today, market=market))
    if history_latest_date >= end_date or start_date > next_trading_date or end_date < today:
        return history_df

    if end_date >= next_trading_date and (start_date > today or history_latest_date >= today):
        live_date = next_trading_date
    else:
        live_date = today
    if history_latest_date >= live_date:
        return history_df
    live_dfs = []
    for ins in ins_list:
        try:
            live_df = get_ticks(ins.order_book_id, start_date=live_date, end_date=live_date, expect_df=True,
                                market=market)
        except (PermissionDenied, MarketNotSupportError):
            pass
        else:
            if live_df is None:
                continue
            if "trading_date" not in live_df.columns:
                live_df["trading_date"] = int8_to_datetime(live_date)
            else:
                live_df["trading_date"] = live_df["trading_date"].apply(to_datetime)
            if ins.type in ("Future", "Option"):
                live_df["change_rate"] = live_df["last"] / live_df["prev_settlement"] - 1
            else:
                live_df["change_rate"] = live_df["last"] / live_df["prev_close"] - 1
            live_df = live_df.reindex(columns=columns)
            live_dfs.append(live_df)

    if not live_dfs:
        return history_df

    if history_df is None:
        return pd.concat(live_dfs)
    return pd.concat([history_df] + live_dfs)


def _convert_int_to_datetime(date_int, time_int):
    return date_int * 1000000000 + time_int


def _adjust_pf(
    pf,
    order_book_ids,
    stocks,
    funds,
    futures888,
    start_date,
    end_date,
    frequency,
    fields,
    has_dominant_id,
    adjust_type,
    skip_suspended,
    market,
):
    adjust = (stocks or funds) and adjust_type in {"pre", "post"}
    if adjust:
        from .detail.adjust_price import adjust_price
        adjust_price(pf, stocks + funds, adjust_type, market)

    if has_dominant_id and futures888:
        pf = add_dominant_id(pf, futures888, frequency)
    if len(order_book_ids) == 1:
        pf = pf.minor_xs(order_book_ids[0])
        if stocks and skip_suspended:
            pf = filter_suspended(pf, order_book_ids[0], start_date, end_date, market)

    if "trading_date" in pf:

        def convert_to_timestamp(v):
            if np.isnan(v):
                return pd.NaT
            return pd.Timestamp(str(int(v)))

        if hasattr(pf.trading_date, "applymap"):
            pf.trading_date = pf.trading_date.applymap(convert_to_timestamp)
        else:
            pf.trading_date = pf.trading_date.apply(convert_to_timestamp)

    if "settlement_direction" in pf:

        def convert_direction(key):
            if np.isnan(key):
                return key
            return SPOT_DIRECTION_MAP[key]

        if hasattr(pf.settlement_direction, "applymap"):
            pf.settlement_direction = pf.settlement_direction.applymap(convert_direction)
        else:
            pf.settlement_direction = pf.settlement_direction.apply(convert_direction)

    if len(fields) == 1 and not has_dominant_id:
        pf = pf[fields[0]]
    if not isinstance(pf, pd.Panel):
        pf.index.name = "date" if frequency == "d" else "datetime"
    return pf


def get_current_trading_date(dt):
    if 7 <= dt.hour < 18:
        return datetime.datetime(year=dt.year, month=dt.month, day=dt.day)
    return get_next_trading_date(dt - datetime.timedelta(hours=4))


def add_dominant_id(result, futures888, frequency):
    d = {}
    for order_book_id in result.minor_axis:
        df = result.minor_xs(order_book_id)
        if order_book_id in futures888.keys() and not df.empty:
            s = get_dominant_future(order_book_id[:-3], df.index[0], df.index[-1])
            if frequency == "d":
                df["dominant_id"] = s
            else:
                df = _add_minbar_dominant_id(df, s)
        else:
            df["dominant_id"] = np.NaN
        d[order_book_id] = df
    result = pd.Panel.from_dict(d, orient="minor")
    return result


def _add_minbar_dominant_id(df, dominant):
    if 'trading_date' in df.columns:
        dominant.index = dominant.index.map(lambda x: float(x.year * 10000 + x.month * 100 + x.day))
        date_dominant_map = dominant.to_dict()
        df['dominant_id'] = df['trading_date'].map(date_dominant_map)
    else:
        date_dominant_map = dominant.to_dict()
        def _set_dominant(dt):
            trading_date = pd.Timestamp(get_current_trading_date(dt))
            return date_dominant_map[trading_date]

        df['dominant_id'] = df.index.map(_set_dominant)
    return df


def filter_suspended(ret, order_book_id, start_date, end_date, market):
    # return a frame if only one order book specified
    s = is_suspended(order_book_id, start_date, end_date, market)
    index = s.index.union(ret.index)
    s = s.reindex(index)
    s = s.fillna(method="ffill")
    s = s.loc[ret.index]
    return ret[s[order_book_id] == False]  # noqa
