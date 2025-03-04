from pathlib import Path
import pandas as pd
from . import schema_utils, mysettings
import os
from tqdm import tqdm
from openpyxl import load_workbook

# load_dotenv()
# folder_path = os.environ.get('RAW_FOLDER_PATH')
folder_path = mysettings.RAW_FOLDER_PATH
print(folder_path)

sub_folder = "converted_file_excel_to_csv"
error_list = []

def is_file_in_subfolder(file_path, parent_folder):
    file_path = Path(file_path).resolve()# Get absolute path  
    parent_folder = Path(parent_folder).resolve()  # Get absolute path
    
    return parent_folder in file_path.parents  # Check if parent_folder is an ancestor

def get_als_lab_folder_name(file_path):
    path_parts = Path(file_path).parts  # Get all parts of the file path
    return path_parts[2] if len(path_parts) > 1 else None  # Return second folder if it exists

def excel_sheets_to_csv(file: Path):
    excel_file = file
    output_folder = file.parent.joinpath(sub_folder)
    # Create the output folder if it doesn't exist
    output_folder.mkdir(parents=True, exist_ok=True) 
    
    try:
        xls = pd.ExcelFile(excel_file)
        wb = load_workbook(excel_file, data_only=True)

        for sheet_name in xls.sheet_names:
            sheet = wb[sheet_name]

            if sheet.sheet_state == "visible":
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                csv_file = f"{output_folder}/{excel_file.name}_{sheet_name}.csv"            
                df.to_csv(csv_file, index=False) 
    except:       
        error_list.append(excel_file)

def get_excel_files():
    """Returns a list of tuples containing the parent folder name and file name for all CSV and Excel files."""
    valid_extensions = (".xls", ".xlsx")
    global folder_path
    folder_path = Path(folder_path)

    for file in tqdm(folder_path.rglob("*")):
        if file.suffix.lower() in valid_extensions:  
            print(f"Converting excel file : {str(file)}")                      
            excel_sheets_to_csv(file)   

    if len(error_list) > 0:
        last_folder = os.path.basename(folder_path)
        output_folder = Path(f"files/{last_folder}")
        # Create the output folder if it doesn't exist
        output_folder.mkdir(parents=True, exist_ok=True) 
        error_output_file_convert_excel_to_csv = f"{output_folder}/error_convert_excel_sheets_to_csv.txt"       

        with open(error_output_file_convert_excel_to_csv, "w") as f:
            for item in error_list:
                f.write(str(item) + "\n")   

schema_key = 1
schema = schema_utils.lookup_schema()
schema_new = {}
flles =  []
schema_type = []
system_source = []
result = {}
files_with_duplicate_columns = []
input_folder_paths_error_files = []

def get_schema_by_folder(input_folder):
    global schema_key

    for existing_key, existing_value in schema.items():
        if existing_value == input_folder:            
            return existing_key    
    return None


def add_header_to_dict(new_list):
    """
    Adds a list to a dictionary, checking for existing values.

    Args:
        dictionary: The dictionary to which the list will be added.
        key: The key to associate with the list in the dictionary.
        new_list: The list to be added to the dictionary.

    Returns:
        - If the list already exists as a value in the dictionary, returns the key associated with the existing list.
        - If the list does not exist, adds the key-value pair to the dictionary and returns None.
    """
    global schema_key

    for existing_key, existing_value in schema.items():
        if sorted(existing_value) == sorted(new_list):            
            return existing_key
    
    for existing_key, existing_value in schema_new.items():
        if sorted(existing_value) == sorted(new_list):            
            return existing_key

    new_key = f'UNknown_Schema_{schema_key}'
    schema_key += 1

    schema_new[new_key] = new_list    
    return new_key

def get_csv_headers(file_path):    
    try:
        df = pd.read_csv(file_path, nrows=0, encoding="utf-8")  # Try UTF-8 first
    except UnicodeDecodeError:
        print(f"⚠️ Encoding error in file: {file_path}. Retrying with Latin-1.")
        df = pd.read_csv(file_path, nrows=0, encoding="latin-1")  # Use Latin-1 as fallback
    columns = list(df.columns)

    if len(set(columns)) == len(columns):
        return columns, True
    else:
        return columns, False    

def get_csv_schema():   
    global  folder_path
    """Returns a list of tuples containing the parent folder name and file name for all CSV and Excel files."""
    valid_extensions = (".csv")
    folder_path = Path(folder_path)

    for file in folder_path.rglob("*"):
        if file.is_file() and file.suffix.lower() in valid_extensions:  
            #print(f"Generating schema for file : {str(file)}")
            
            file_in_folder = False  
            input_folder_paths = schema_utils.get_input_folder_list()

            for input_folder_path in input_folder_paths:                
                file_in_folder = is_file_in_subfolder(file, input_folder_path)
                if file_in_folder:
                    break
            als_lab = get_als_lab_folder_name(file)

            if file_in_folder:
                get_schema = get_schema_by_folder(input_folder_path)

                if get_schema:
                    schema_type.append(get_schema)
                    flles.append(file)
                    system_source.append(als_lab)
                else:
                    input_folder_paths_error_files.append(file)
            else:
                csv_column_names, is_unique = get_csv_headers(file)
                
                if is_unique:
                    get_schema = add_header_to_dict(csv_column_names)
                    schema_type.append(get_schema)

                    flles.append(file)
                    system_source.append(als_lab)
                else:
                    files_with_duplicate_columns.append(file)
    
    result['file_name'] = flles
    result['schema_type'] = schema_type
    result['system_source'] = system_source

    if len(files_with_duplicate_columns) > 0:
        last_folder = os.path.basename(folder_path)
        output_folder = Path(f"files/{last_folder}")
        # Create the output folder if it doesn't exist
        output_folder.mkdir(parents=True, exist_ok=True) 
        error_output_file_csv_with_duplicate_columns = f"{output_folder}/error_csv_with_duplicate_columns.txt"  
        
        with open(error_output_file_csv_with_duplicate_columns, "w") as f:
            for item in files_with_duplicate_columns:
                f.write(str(item) + "\n")   

    if len(input_folder_paths_error_files) > 0:
        last_folder = os.path.basename(folder_path)
        output_folder = Path(f"files/{last_folder}")
        # Create the output folder if it doesn't exist
        output_folder.mkdir(parents=True, exist_ok=True) 
        error_output_file_csv_from_input_folder = f"{output_folder}/error_csv_from_input_folder.txt" 

        with open(error_output_file_csv_from_input_folder, "w") as f:
            for item in files_with_duplicate_columns:
                f.write(str(item) + "\n") 

    df = pd.DataFrame(result)

    last_folder = os.path.basename(folder_path)    
    output_folder = Path(f"files/{last_folder}")
    # Create the output folder if it doesn't exist
    output_folder.mkdir(parents=True, exist_ok=True) 
    file_with_schema_filename = f"{output_folder}/file_with_schema.csv"
    df.to_csv(file_with_schema_filename, index=False) 

    if len(schema_new) > 0:
        last_folder = os.path.basename(folder_path)
        output_folder = Path(f"files/{last_folder}")
        # Create the output folder if it doesn't exist
        output_folder.mkdir(parents=True, exist_ok=True) 
        output_file_new_schema = f"{output_folder}/schema.txt" 
        with open(output_file_new_schema, "w") as file:
            for key, value in schema_new.items():
                file.write(f"{key}: {value}\n")

    return df

