# -*- coding: utf-8 -*-
import warnings
import pandas as pd

from rqdatac.client import get_client
from rqdatac.validators import (
    ensure_int,
    ensure_date_int,
    ensure_order_book_id,
    ensure_order_book_ids,
)
from rqdatac.utils import to_datetime
from rqdatac.decorators import export_as_api, ttl_cache

INS_COLUMNS = [
    "order_book_id",
    "symbol",
    "full_name",
    "exchange",
    "bond_type",
    "trade_type",
    "value_date",
    "maturity_date",
    "par_value",
    "coupon_rate",
    "coupon_frequency",
    "coupon_method",
    "compensation_rate",
    "total_issue_size",
    "de_listed_date",
    "stock_code",
    "conversion_start_date",
    "conversion_end_date",
    "redemption_price",
    "issue_price",
    "call_protection",
    "listed_date"
]


class Instrument:
    def __init__(self, attrs):
        self.__dict__.update(attrs)
        self.__cache = {}

    def __str__(self):
        return "{}(\n{}\n)".format(
            type(self).__name__,
            ",\n".join(["{}={!r}".format(k, v) for k, v in self.items() if not k.startswith("_")]),
        )

    __repr__ = __str__

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, item):
        return self.__dict__[item]

    def get(self, item, default=None):
        return self.__dict__.get(item, default)

    def items(self):
        return self.__dict__.items()

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def __cache_get(self, v):
        return self.__cache.get(v)

    def __cache_set(self, k, v):
        self.__cache[k] = v

    def coupon_rate_table(self):
        """变动利率可转债信息"""
        if "coupon_rate_table" in self.__cache:
            return self.__cache_get("coupon_rate_table")
        info = get_client().execute("convertible.get_coupon_rate_table", self.order_book_id)
        info = pd.DataFrame(info).set_index(['start_date', 'end_date']) if info else None
        self.__cache_set("coupon_rate_table", info)
        return info

    def option(self, option_type=None):
        if option_type is not None:
            option_type = ensure_int(option_type)
            if option_type not in (1, 2, 3, 4, 5, 6, 7):
                raise ValueError("option_type: expect value in (None, 1, 2, 3, 4, 5, 6, 7)")

        data = get_client().execute("convertible.option", self.order_book_id, option_type)
        if not data:
            return

        df = pd.DataFrame(data)
        if 'payment_year' in df.columns:
            sort_fields = ['option_type', 'payment_year']
        else:
            sort_fields = ['option_type']
        df = df.sort_values(sort_fields).reset_index()
        column_order = ['option_type', 'start_date', 'end_date', 'payment_year', 'level', 'window_days',
                        'reach_days', 'frequency', 'price', 'if_include_interest', 'remark']
        column = [i for i in column_order if i in df.columns]
        return df[column]


@ttl_cache(12 * 3600)
def _all_instruments_dict(market="cn"):
    return {
        i['order_book_id']: Instrument(i)
        for i in get_client().execute("convertible.all_instruments", market=market)
    }


@export_as_api(namespace="convertible")
def all_instruments(date=None, market="cn"):
    """获取所有可转债详细信息

    :param market:  (Default value = "cn")
    :returns: DataFrame
    """
    profile = lambda v: (
        v.order_book_id,
        v.symbol,
        v.full_name,
        v.exchange,
        v.bond_type,
        v.trade_type,
        v.value_date,
        v.maturity_date,
        v.par_value,
        v.coupon_rate,
        v.coupon_frequency,
        v.coupon_method,
        v.compensation_rate,
        v.total_issue_size,
        v.de_listed_date,
        v.stock_code,
        v.conversion_start_date,
        v.conversion_end_date,
        v.redemption_price,
        v.issue_price,
        v.call_protection,
        v.listed_date,
    )

    def judge(listed_date, de_listed_date):
        if listed_date and de_listed_date:
            return listed_date <= date and de_listed_date > date
        if listed_date:
            return listed_date <= date
        else:
            return False

    if date:
        date = to_datetime(date)
        data = [profile(v) for v in _all_instruments_dict(market).values() if judge(v.listed_date, v.de_listed_date)]
    else:
        data = [profile(v) for v in _all_instruments_dict(market).values()]
    df = pd.DataFrame(
        data,
        columns=INS_COLUMNS,
    )
    df.sort_values('order_book_id', inplace=True)
    return df.reset_index(drop=True)


@export_as_api(namespace="convertible")
def instruments(order_book_ids, market="cn"):
    """获取可转债详细信息

    :param order_book_ids: 可转债代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: Instrument object or list of Instrument object
            取决于参数是一个 order_book_id 还是多个 order_book_id
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    all_dict = _all_instruments_dict(market)
    if len(order_book_ids) == 1:
        try:
            return all_dict[order_book_ids[0]]
        except KeyError:
            warnings.warn('unknown convertible order_book_id: {}'.format(order_book_ids))
            return
    all_list = (all_dict.get(i) for i in order_book_ids)
    return [i for i in all_list if i]


@export_as_api(namespace="convertible")
def get_cash_flow(order_book_id, start_date=None, end_date=None, market="cn"):
    """获取现金流信息

    :param order_book_id: 可转债ID str
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param market:  (Default value = "cn")
    :return: pd.DataFrame
    """
    order_book_id = ensure_order_book_id(order_book_id)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_cash_flow", order_book_id, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "payment_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_call_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取赎回信息

    :param order_book_ids: 可转债ID，str or list
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param market:  (Default value = "cn")
    :return: pd.DataFrame
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_call_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_put_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取回售信息

    :param order_book_ids: 可转债ID，str or list
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param market:  (Default value = "cn")
    :return: pd.DataFrame
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_put_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_conversion_price(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取转股价变动信息

    :param order_book_ids: 可转债ID，str or list
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param market:  (Default value = "cn")
    :return: pd.DataFrame
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_conversion_price", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_conversion_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取转股变动信息

    :param order_book_ids: 可转债ID，str or list
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param market:  (Default value = "cn")
    :return: pd.DataFrame
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_conversion_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df
