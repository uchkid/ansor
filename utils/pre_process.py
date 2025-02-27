from pathlib import Path
import pandas as pd
from . import pre_process_function

def get_second_folder_name_and_file_name(file_path):
    path_parts = Path(file_path).parts  # Get all parts of the file path
    path_part = path_parts[1] if len(path_parts) > 1 else None  # Return second folder if it exists
    file_name = Path(file_path).stem
    return path_part, file_name

def preprocess (df):
    #Get the preprocess function for each file
    no_of_files = len(df)
    for i in range (len(df)):
        file_path = df.iloc[i,0]
        schema_type = df.iloc[i,1]
        als_lab = df.iloc[i,2]

        print(f'preprocess file {i+1} of {no_of_files} [{file_path}]')
        processed_df = pre_process_function.preprocess(file_path,schema_type,als_lab)

        if processed_df is not None:
            parent_folder, file_name = get_second_folder_name_and_file_name(file_path)

            if parent_folder:
                output_folder = Path(f"data/pre_processed/{parent_folder}")
            else:
                output_folder = Path(f"data/pre_processed")

            output_folder.mkdir(parents=True, exist_ok=True) 
            preprocessed_output_file = f"{output_folder}/{file_name}.csv"            
            processed_df.to_csv(preprocessed_output_file, index=False)  
        else:
            print(f"file not processed {file_path}")           
    

        

