import os
import shutil
import pandas as pd
from functions import *

if __name__ == "__main__":
    reports_directory = 'annual_report'
    all_df = {}
    years = [d for d in os.listdir(reports_directory) if d != 'final' and os.path.isdir(
        os.path.join(reports_directory, d))]
    for year in years:
        for state in os.listdir(f'{reports_directory}/{year}'):
            for file in os.listdir(f'{reports_directory}/{year}/{state}'):
                file_path = f'{reports_directory}/{year}/{state}/{file}'
                if os.path.isfile(file_path) and '.parquet' in file:
                    table = file.replace('.parquet', '')
                    df = pd.read_parquet(file_path)
                    df['Year'] = int(year)
                    df['State'] = int(state)
                    if table not in all_df.keys():
                        all_df[table] = [df]
                    else:
                        all_df[table].append(df)
    for table in all_df.keys():
        final_df = pd.concat(all_df[table], ignore_index=True)
        final_table_path = f'{reports_directory}/final/{table}'
        #Delete used because if we run code twice it duplicates, 
        # I expect this is because of how pandas handles writing to partitioned parquet
        if os.path.exists(final_table_path):
            shutil.rmtree(final_table_path)
        check_and_create_folder(final_table_path)
        final_df.to_parquet(final_table_path,
                            engine='pyarrow', partition_cols=['Year'])
