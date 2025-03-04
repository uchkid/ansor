from pathlib import Path
import os
import pandas as pd
from . import pre_process_function, mysettings
from tqdm import tqdm

folder_path = mysettings.RAW_FOLDER_PATH

def get_als_lab_folder_name_and_file_name(file_path):
    path_parts = Path(file_path).parts  # Get all parts of the file path
    path_part = path_parts[2] if len(path_parts) > 1 else None  # Return second folder if it exists
    file_name = Path(file_path).stem
    return path_part, file_name

def get_folder_names(folder_path):
    """
    Returns a list of folder names within the given folder path using pathlib.

    Args:
        folder_path (str or Path): The path to the folder.

    Returns:
        list: A list of folder names.
    """
    folder_path = Path(folder_path)
    folder_names = []
    try:
        for item in folder_path.iterdir():
            if item.is_dir():
                folder_names.append(item.name)
    except FileNotFoundError:
        print(f"Folder not found: {folder_path}")
    return folder_names

combine_lookup = mysettings.LOOKUP_COMBINE_PREPROCESS_FUNCTION

def preprocess (df):
    #Get the preprocess function for each file
    last_folder = os.path.basename(folder_path)
    no_of_files = len(df)
    for i in tqdm(range (len(df))):
        file_path = df.iloc[i,0]
        schema_type = df.iloc[i,1]
        als_lab = df.iloc[i,2]

        print(f'preprocess file {i+1} of {no_of_files} [{file_path}]')
        processed_df, func_name = pre_process_function.preprocess(file_path,schema_type,als_lab)

        if processed_df is not None:
            parent_folder, file_name = get_als_lab_folder_name_and_file_name(file_path)   
            combine_folder = combine_lookup.get(func_name, "combine")         
            if parent_folder:
                output_folder = Path(f"data/pre_processed/{last_folder}/{combine_folder}/{parent_folder}")
            else:
                output_folder = Path(f"data/pre_processed/{last_folder}/{combine_folder}")

            output_folder.mkdir(parents=True, exist_ok=True) 
            preprocessed_output_file = f"{output_folder}/{file_name}.csv"            
            processed_df.to_csv(preprocessed_output_file, index=False)  
        else:
            print(f"file not processed {file_path}")           
    
def preprocess_ashford():
    processed_df = pre_process_function.preprocess_labtrac_ashford()

    output_folder = Path(f"data/pre_processed/sales/Ashford")
    output_folder.mkdir(parents=True, exist_ok=True) 
    preprocessed_output_file = f"{output_folder}/ashford_preprocess.csv"            
    processed_df.to_csv(preprocessed_output_file, index=False)  

def preprocess_densign():
    processed_df = pre_process_function.preprocess_evident_densign("Densign")

    output_folder = Path(f"data/pre_processed/sales/Densign")
    output_folder.mkdir(parents=True, exist_ok=True) 
    preprocessed_output_file = f"{output_folder}/densign_preprocess.csv"            
    processed_df.to_csv(preprocessed_output_file, index=False) 

def combine_preprocess():
    valid_extensions = (".csv")
    last_folder = os.path.basename(folder_path)
    preprocessed_folder_path = Path(f"data/pre_processed/{last_folder}")
    preprocessed_folder_names = get_folder_names(preprocessed_folder_path)

    for folder_name in tqdm(preprocessed_folder_names):
        print(f'Combining data in {folder_name}')
        combine_preprocessed_folder_path = Path(f"{preprocessed_folder_path}/{folder_name}")
        combine_df =  pd.DataFrame()

        for file in combine_preprocessed_folder_path.rglob("*"):
            if file.is_file() and file.suffix.lower() in valid_extensions:  
                try:
                    df = pd.read_csv(file, encoding="utf-8")  # Try UTF-8 first
                except UnicodeDecodeError:
                    print(f"⚠️ Encoding error in file: {file}. Retrying with Latin-1.")
                    df = pd.read_csv(file, encoding="latin-1")  # Use Latin-1 as fallback

                combine_df = pd.concat([combine_df,df])
        
        combined_output_folder = Path(f"data/pre_processed_combined/{last_folder}")
        combined_output_folder.mkdir(parents=True, exist_ok=True) 
        combined_output_file = f"{combined_output_folder}/combined_{folder_name}.csv"            
        combine_df.to_csv(combined_output_file, index=False) 



