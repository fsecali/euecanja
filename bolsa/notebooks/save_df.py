import pandas as pd
from datetime import datetime as dt

class WrongType(Exception):
    pass

def df2file(df, prefix, path='../src/data/', formatting='parquet'):
    "Save Dataframe to a file in a given path using parquet or csv"
    if isinstance(df, pd.DataFrame):
        datetime = dt.now().strftime('%Y%m%d%H%M%S')
        name = '_'.join( (prefix,datetime) )
        if formatting == 'parquet':
            df.to_parquet(path + name + '.parquet')
        elif formatting == 'csv':
            df.to_csv(path + name + '.csv')
    else:
        raise WrongType

if __name__ == '__main__':
    pass
