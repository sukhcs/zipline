import os

import yaml

CONFIG_PATH = os.environ.get("ZIPLINE_TRADER_CONFIG")
with open(CONFIG_PATH, mode='r') as f:
    ZIPLINE_CONFIG = yaml.safe_load(f)


def get_alpaca_config():
    return ZIPLINE_CONFIG["alpaca"]


class AlphaVantage:
    av = ZIPLINE_CONFIG["alpha-vantage"]

    @property
    def sample_frequency(self):
        """
        how long to wait between samples. default for free accounts - 1 min.
        so we could do 5 sample per minute.
        you could define it in the config file or override it with env variable
        :return:
        """
        val = 60
        if os.environ.get('AV_FREQ_SEC'):
            val = int(os.environ.get('AV_FREQ_SEC'))
        elif self.av.get('AV_FREQ_SEC'):
            val = int(self.av.get('AV_FREQ_SEC'))
        return val

    @property
    def max_calls_per_freq(self):
        """
        max api calls you could do per frequency period.
        free account can do 5 calls per minute
        you could define it in the config file or override it with env variable
        :return:
        """
        val = 5
        if os.environ.get('AV_CALLS_PER_FREQ'):
            val = int(os.environ.get('AV_CALLS_PER_FREQ'))
        elif self.av.get('AV_CALLS_PER_FREQ'):
            val = int(self.av.get('AV_CALLS_PER_FREQ'))
        return val

    @property
    def breathing_space(self):
        """
        to make sure we don't pass the limit we take some breathing room for sampling error.
        you could define it in the config file or override it with env variable
        :return:
        """
        val = 1
        if os.environ.get('AV_TOLERANCE_SEC'):
            val = int(os.environ.get('AV_TOLERANCE_SEC'))
        elif self.av.get('AV_TOLERANCE_SEC'):
            val = int(self.av.get('AV_TOLERANCE_SEC'))
        return val

    @property
    def api_key(self):
        """
        api key for alpha vantage
        you could define it in the config file or override it with env variable
        :return:
        """
        val = None
        if os.environ.get('ALPHAVANTAGE_API_KEY'):
            val = os.environ.get('ALPHAVANTAGE_API_KEY')
        elif self.av.get('ALPHAVANTAGE_API_KEY'):
            val = self.av.get('ALPHAVANTAGE_API_KEY')
        if not val:
            raise Exception("Alpha Vantage key is not set by user")
        return val


def get_binance_config():
    return ZIPLINE_CONFIG["binance"]


if __name__ == '__main__':
    print(ZIPLINE_CONFIG)
    print(get_alpaca_config())
    av_conf = AlphaVantage()
    print(av_conf.sample_frequency)
    print(av_conf.max_calls_per_freq)
    print(get_binance_config())
