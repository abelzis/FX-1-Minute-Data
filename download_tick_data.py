import csv
import os
import argparse

from histdata.api import download_hist_data, TimeFrame as TF


def mkdir_p(path):
    import errno
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def download_tick_data(dirpath: str, platform: str, pair_name: str):
    with open('pairs.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            currency_pair_name, pair, history_first_trading_month = row
            if (currency_pair_name != pair_name):
                continue
            year = int(history_first_trading_month[0:4])
            month = int(history_first_trading_month[4:6])
            output_folder = os.path.join(dirpath, pair)
            mkdir_p(output_folder)
            try:
                download_year = year
                while True:
                    download_month = 1
                    if (download_year == year):
                        download_month = month
                    while download_month <= 12:
                        print('-', download_hist_data(year=download_year,
                                                      month=download_month,
                                                      pair=pair,
                                                      output_directory=output_folder,
                                                      verbose=False,
                                                      platform=platform,
                                                      time_frame=TF.TICK_DATA))
                        download_month += 1
                    download_year += 1
            except Exception as err:
                print(err)
                print('[DONE] for currency', currency_pair_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download tick data')
    parser.add_argument('--dir', metavar='path', required=True,
                        help='directory for the data')
    parser.add_argument('--platform', metavar='path', default="ASCII",
                        help='name of the platform (e.g.: ASCII)')
    parser.add_argument('--pair', metavar='path', required=True,
                        help='name of the pair')
    args = parser.parse_args()
    download_tick_data(
        dirpath=args.dir, platform=args.platform, pair_name=args.pair)
