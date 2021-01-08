import pandas as pd
from zipline.data import bundles

from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.data.data_portal import DataPortal
from zipline.utils.calendars import get_calendar
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine

BUNDLE_DATA = None
PRICING_LOADER = None

def set_bundle_data(bundle_name='alpaca_api'):
    global BUNDLE_DATA, PRICING_LOADER
    BUNDLE_DATA = bundles.load(bundle_name)

    PRICING_LOADER = USEquityPricingLoader.without_fx(BUNDLE_DATA.equity_daily_bar_reader,
                                                          BUNDLE_DATA.adjustment_reader)

def choose_loader(column):
    """ Define the function for the get_loader parameter
     Set the dataloader"""

    if column not in USEquityPricing.columns:
        raise Exception('Column not in USEquityPricing')
    return PRICING_LOADER


def create_data_portal(_bundle_name, _trading_calendar, start_date):
    global BUNDLE_DATA
    if not BUNDLE_DATA:
        set_bundle_data(_bundle_name)
    # Create a data portal
    data_portal = DataPortal(BUNDLE_DATA.asset_finder,
                             trading_calendar=_trading_calendar,
                             first_trading_day=start_date,
                             equity_daily_reader=BUNDLE_DATA.equity_daily_bar_reader,
                             adjustment_reader=BUNDLE_DATA.adjustment_reader)
    return data_portal


def get_pricing(data_portal, trading_calendar, assets, start_date, end_date, field='close'):
    # Set the given start and end dates to Timestamps. The frequency string C is used to
    # indicate that a CustomBusinessDay DateOffset is used
    end_dt = pd.Timestamp(end_date, tz='UTC', freq='C')
    start_dt = pd.Timestamp(start_date, tz='UTC', freq='C')

    # Get the locations of the start and end dates
    end_loc = trading_calendar.closes.index.get_loc(end_dt)
    start_loc = trading_calendar.closes.index.get_loc(start_dt)

    # return the historical data for the given window
    return data_portal.get_history_window(assets=assets, end_dt=end_dt, bar_count=end_loc - start_loc,
                                          frequency='1d',
                                          field=field,
                                          data_frequency='daily')


def create_pipeline_engine(bundle_name='alpaca_api'):
    global BUNDLE_DATA
    if not BUNDLE_DATA:
        set_bundle_data(bundle_name)
    # Create a Pipeline engine
    engine = SimplePipelineEngine(get_loader=choose_loader,
                                  asset_finder=BUNDLE_DATA.asset_finder)
    return engine
