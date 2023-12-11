#import the library
import argparse as parserlib
from pathlib import Path

import numpy as np
import pandas as pd
import datetime as dt


def processdata(to_process: str, output_dir: str):
    # Use a breakpoint in the code line below to debug your script.
    parquet_file = Path(to_process)
    output_path= Path(output_dir)
    table = pd.read_parquet(parquet_file)
    df = pd.DataFrame(data=table)
    df.timestamp = pd.to_datetime(df['timestamp'])
    timestampdiff = df['timestamp'].diff()
    timestampdiffcheck = (timestampdiff / np.timedelta64(1, 'h')) > 7
    df['timestampdiffcheck'] = timestampdiffcheck
    df2 = df.query('timestampdiffcheck == True')
    df2_index = df2.index.to_numpy()
    df.timestamp = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    header = ["latitude", "longitude", "timestamp"]
    for i in range(len(df2_index) - 1):
        if i < len(df2_index):
            df3 = df.loc[df2_index[i]:df2_index[i + 1]]
            unit = df3['unit'].sample(1).to_string(index=False, header=False)
            print(output_path/'{unit}_{i}.csv'.format(i=i, unit=unit))
            df3.to_csv(output_path/'{unit}_{i}.csv'.format(i=i, unit=unit), index=False, columns=header)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        parser=parserlib.ArgumentParser()
        parser.add_argument("to_process", help="Path to the Parquet file to be processed.")
        parser.add_argument("output_dir", help="The folder to store the resulting CSV files.")
        args=parser.parse_args()
        filepath = str(args.to_process)
        outputpath = str(args.output_dir)

        processdata(filepath,outputpath)
    except Exception as e:
        print("Exception occured:",e)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
