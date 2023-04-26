import argparse
import os
import pandas as pd
import fixdata

OUTPUT_PATH = "cleantick"
COLNAMES = ['datetime_str', 'bid', 'ask', 'volume']


def walk_through_data(dirpath: str, output_format: str):
    for root, _, files in os.walk(dirpath):
        output_dir = root.replace(dirpath, OUTPUT_PATH, 1)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(root)

        for file in sorted(files):
            file_input = os.path.join(root, file)

            splitted_file_name = os.path.splitext(file_input)

            if splitted_file_name[1] != ".csv":
                continue

            print(f"---Parsing file {file_input}---")

            csv_df = pd.read_csv(file_input, names=COLNAMES,
                                 header=None, delimiter=",")

            clean_data(csv_df, file_input, dirpath, output_format)


def clean_data(data_df: pd.DataFrame, file_input: str,
               dirpath: str, output_format: str):

    fixed_df = clean_csv(data_df)

    file_output = file_input.replace(
        dirpath, OUTPUT_PATH, 1).replace(".csv", "", 1)

    fixdata.write_output(fixed_df, file_output, output_format)


def clean_csv(data_df: pd.DataFrame):

    data_df["datetime"] = pd.to_datetime(
        data_df["datetime_str"], format="%Y%m%d %H%M%S%f").dt.tz_localize('-0500')

    data_df = filter_time(data_df)

    csv_df = data_df.drop("datetime_str", axis=1)

    csv_df['unix'] = csv_df.apply(
        lambda x: fixdata.datetime_to_unix(x['datetime']), axis=1)

    csv_df["datetime"] = csv_df["unix"]
    csv_df = csv_df.drop("unix", axis=1).set_index("datetime")

    return csv_df


def filter_time(data_df: pd.DataFrame):
    data_df_hour = data_df['datetime'].dt.hour
    data_df_minute = data_df['datetime'].dt.minute

    df_filter = ((data_df_hour == 9) & (data_df_minute >= 25)) | (
        (data_df_hour == 10) & (data_df_minute <= 35))

    return data_df.loc[df_filter]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Clean histdata forex tick data')
    parser.add_argument('--dir', metavar='path', required=True,
                        help='directory for the data')
    parser.add_argument('--output_format', metavar='path', default="csv",
                        help='output data format')
    args = parser.parse_args()

    walk_through_data(dirpath=args.dir, output_format=args.output_format)
