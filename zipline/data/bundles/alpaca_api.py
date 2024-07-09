import collections
import alpaca_trade_api as tradeapi
from datetime import timedelta, time as dtime
import numpy as np
from pathlib import Path
import pandas as pd
import pytz
from alpaca_trade_api.common import URL
from dateutil import tz
from trading_calendars import TradingCalendar

import zipline.config
from zipline.data.bundles import core as bundles
from zipline.data.bundles.common import asset_to_sid_map
from zipline.data.bundles.universe import Universe, all_alpaca_assets, get_sp500, get_sp100, get_nasdaq100
from dateutil.parser import parse as date_parse

user_home = str(Path.home())

CLIENT: tradeapi.REST = None
NY = "America/New_York"


def initialize_client():
    global CLIENT
    conf = zipline.config.bundle.AlpacaConfig()
    key = conf.key
    secret = conf.secret
    base_url = conf.base_url
    CLIENT = tradeapi.REST(key_id=key,
                           secret_key=secret,
                           base_url=URL(base_url))

ASSETS = None
def list_assets():
    global ASSETS
    if not ASSETS:
        conf = zipline.config.bundle.AlpacaConfig()
        custom_asset_list = conf.custom_asset_list
        if custom_asset_list:
            custom_asset_list = custom_asset_list.strip().replace(" ", "").split(",")
            ASSETS = list(set(custom_asset_list))
        else:
            try:
                universe = Universe[conf.universe]
            except:
                universe = Universe.ALL
            if universe == Universe.ALL:
                ASSETS = all_alpaca_assets(CLIENT)
            elif universe == Universe.SP100:
                ASSETS = get_sp100()
            elif universe == Universe.SP500:
                ASSETS = get_sp500()
            elif universe == Universe.NASDAQ100:
                ASSETS = get_nasdaq100()
            ASSETS = list(set(ASSETS))
    return ASSETS


def iso_date(date_str):
    """
    this method will make sure that dates are formatted properly
    as with isoformat
    :param date_str:
    :return: YYYY-MM-DD date formatted
    """
    return date_parse(date_str).date().isoformat()


def get_aggs_from_alpaca(symbols,
                         start,
                         end,
                         granularity,
                         compression=1):
    """
    https://alpaca.markets/docs/api-documentation/api-v2/market-data/bars/
    Alpaca API as a limit of 1000 records per api call. meaning, we need to
    do multiple calls to get all the required data if the date range is
    large.
    also, the alpaca api does not support compression (or, you can't get
    5 minute bars e.g) so we need to resample the received bars.
    also, we need to drop out of market records.
    this function does all of that.

    note:
    this was the old way of getting the data
      response = CLIENT.get_aggs(dataname,
                                    compression,
                                    granularity,
                                    self.iso_date(start_dt),
                                    self.iso_date(end_dt))
      the thing is get_aggs work nicely for days but not for minutes, and
      it is not a documented API. barset on the other hand does
      but we need to manipulate it to be able to work with it
      smoothly and return data the same way polygon does
    """

    def _iterate_api_calls():
        """
        you could get max 1000 samples from the server. if we need more
        than that we need to do several api calls.

        currently the alpaca api supports also 5Min and 15Min so we could
        optimize server communication time by addressing timeframes
        """
        got_all = False
        curr = end
        response: pd.DataFrame = pd.DataFrame([])
        while not got_all:
            if granularity == 'minute' and compression == 5:
                timeframe = "5Min"
            elif granularity == 'minute' and compression == 15:
                timeframe = "15Min"
            else:
                timeframe = granularity
            r = CLIENT.get_barset(symbols,
                                  timeframe,
                                  limit=1000,
                                  end=curr.isoformat()
                                  )
            if r:
                response = r.df if response.empty else pd.concat([r.df, response])
                response.sort_index(inplace=True)
                if response.index[0] <= (pytz.timezone(NY).localize(
                        start) if not start.tzname() else start):
                    got_all = True
                else:
                    delta = timedelta(days=1) if granularity == "day" \
                        else timedelta(minutes=1)
                    curr = response.index[0] - delta
            else:
                # no more data is available, let's return what we have
                break
        return response

    def _fillna(df, granularity, start, end):
        if granularity != 'day':
            return df
        if df.empty:
            return df
        calendar: TradingCalendar = trading_calendars.get_calendar("NYSE")
        last_val = df.iloc[0]
        current = start
        while current <= end:
            if calendar.is_session(current):
                if current.replace(tzinfo=tz.gettz(NY)) in df.index:
                    last_val = df.loc[current.replace(tzinfo=tz.gettz(NY))]
                else:
                    # df.loc[pytz.timezone(NY).localize(current)] = last_val
                    df.loc[current.replace(tzinfo=tz.gettz(NY))] = last_val
            current += timedelta(days=1)
        return df

    def _clear_out_of_market_hours(df):
        """
        only interested in samples between 9:30, 16:00 NY time
        """
        return df.between_time("09:30", "16:00")

    def _drop_early_samples(df):
        """
        samples from server don't start at 9:30 NY time
        let's drop earliest samples
        """
        for i, b in df.iterrows():
            if i.time() >= dtime(9, 30):
                return df[i:]

    def _resample(df):
        """
        samples returned with certain window size (1 day, 1 minute) user
        may want to work with different window size (5min)
        """

        if granularity == 'minute':
            sample_size = f"{compression}Min"
        else:
            sample_size = f"{compression}D"
        df = df.resample(sample_size).agg(
            collections.OrderedDict([
                ('open', 'first'),
                ('high', 'max'),
                ('low', 'min'),
                ('close', 'last'),
                ('volume', 'sum'),
            ])
        )
        if granularity == 'minute':
            return df.between_time("09:30", "16:00")
        else:
            return df

    if not start:
        response = CLIENT.get_barset(symbols,
                                     granularity,
                                     limit=1000,
                                     end=end).df
    else:
        response = _iterate_api_calls()

    cdl = response
    if granularity == 'minute':
        cdl = _clear_out_of_market_hours(cdl)
        cdl = _drop_early_samples(cdl)
    if compression != 1:
        response = _resample(cdl)
    # response = _back_to_aggs(cdl)
    else:
        response = cdl
    if granularity == 'day':
        response = response[start:end]  # we only want data between dates
    processed = pd.DataFrame([], columns=response.columns)
    for sym in response.columns.levels[0]:
        df: pd.DataFrame = response[sym]
        df = df.dropna()
        df = _fillna(df, granularity, start, end)
        if processed.empty and not df.empty:
            processed = processed.reindex(df.index.values)
        if not df.empty:
            processed[sym] = df

    return processed

MAX_PER_REQUEST_AMOUNT = 200  # Alpaca max symbols per 1 http request
def df_generator(interval, start, end, assets_to_sids):
    exchange = 'NYSE'
    asset_list = list_assets()
    base_sid = 0
    # some symbols from alpaca are duplicated, which causes an issue with zipline
    # ingest process. for now, we make sure we serve one of them (for now the first one)
    already_ingested = {}
    for i in range(len(asset_list[::MAX_PER_REQUEST_AMOUNT])):
        partial = asset_list[MAX_PER_REQUEST_AMOUNT*i:MAX_PER_REQUEST_AMOUNT*(i+1)]
        df: pd.DataFrame = get_aggs_from_alpaca(partial, start, end, 'day' if interval == '1d' else 'minute', 1)
        for _, symbol in enumerate(df.columns.levels[0]):
            try:
                sid = assets_to_sids[symbol]
                # doing this makes sure not all data in df is null
                # isnull returns 0 and 1 matrix.
                # doing sum twice, makes sure there isn't even one NaN value
                # and since we do ffill of the data, that should not happen
                # if df[symbol].isnull().sum().sum() == 0:
                if not df[symbol].isnull().all().all():
                    if symbol not in already_ingested:
                        first_traded = start
                        auto_close_date = end + pd.Timedelta(days=1)
                        yield (sid, df[symbol].sort_index()), symbol, start, end, first_traded, auto_close_date, exchange
                        already_ingested[symbol] = True

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"error while processig {(sid + base_sid, symbol)}: {e}")


def metadata_df():
    metadata_dtype = [
        ('symbol', 'object'),
        # ('asset_name', 'object'),
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('first_traded', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange', 'object'), ]
    metadata_df = pd.DataFrame(
        np.empty(len(list_assets()), dtype=metadata_dtype))

    return metadata_df


@bundles.register('alpaca_api', calendar_name="NYSE", minutes_per_day=390)
def api_to_bundle(interval=['1m']):
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):

        assets_to_sids = asset_to_sid_map(asset_db_writer.asset_finder, list_assets())

        def minute_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1m',
                                                                                   start=start_session,
                                                                                   end=end_session,
                                                                                   assets_to_sids=assets_to_sids))

        def daily_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1d',
                                                                                   start=start_session,
                                                                                   end=end_session,
                                                                                   assets_to_sids=assets_to_sids))
        for _interval in interval:
            metadata = metadata_df()
            if _interval == '1d':
                daily_bar_writer.write(daily_data_generator(), assets=assets_to_sids.values(), show_progress=True)
            elif _interval == '1m':
                minute_bar_writer.write(
                    minute_data_generator(), assets=assets_to_sids.values(), show_progress=True)

            # Drop the ticker rows which have missing sessions in their data sets
            metadata.dropna(inplace=True)

            asset_db_writer.write(equities=metadata)
            print(metadata)
            adjustment_writer.write()

    return ingest


if __name__ == '__main__':
    from zipline.data.bundles import register
    from zipline.data import bundles as bundles_module
    import trading_calendars
    import os

    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    end_date = pd.Timestamp('now', tz='utc').date() - timedelta(days=1)
    while not cal.is_session(str(end_date)):
        end_date -= timedelta(days=1)
    end_date = pd.Timestamp(end_date, tz='utc')

    # start_date = pd.Timestamp('2020-10-03 0:00', tz='utc')
    # while not cal.is_session(start_date):
    #     start_date += timedelta(days=1)

    start_date = end_date - timedelta(days=365)
    while not cal.is_session(start_date):
        start_date -= timedelta(days=1)

    initialize_client()

    import time

    start_time = time.time()

    register(
        'alpaca_api',
        # api_to_bundle(interval=['1d', '1m']),
        # api_to_bundle(interval=['1m']),
        api_to_bundle(interval=['1d']),
        calendar_name='NYSE',
        start_session=start_date,
        end_session=end_date
    )

    assets_version = ((),)[0]  # just a weird way to create an empty tuple
    bundles_module.ingest(
        "alpaca_api",
        os.environ,
        assets_versions=assets_version,
        show_progress=True,
    )

    print(f"--- It took {timedelta(seconds=time.time() - start_time)} ---")
