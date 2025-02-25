from pathlib import Path
import pandas as pd
from . import schema_utils

sub_folder = "converted_file_excel_to_csv"
error_list = []

def excel_sheets_to_csv(file: Path):
    excel_file = file
    output_folder = file.parent.joinpath(sub_folder)
    # Create the output folder if it doesn't exist
    output_folder.mkdir(parents=True, exist_ok=True) 
    
    try:
        xls = pd.ExcelFile(excel_file)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            csv_file = f"{output_folder}/{excel_file.name}_{sheet_name}.csv"            
            df.to_csv(csv_file, index=False) 
    except:       
        error_list.append(excel_file)

def get_excel_files(folder_path):
    """Returns a list of tuples containing the parent folder name and file name for all CSV and Excel files."""
    valid_extensions = (".xls", ".xlsx")
    folder_path = Path(folder_path)

    for file in folder_path.rglob("*"):
        if file.suffix.lower() in valid_extensions:  
            print(f"Converting excel file : {str(file)}")                      
            excel_sheets_to_csv(file)   

    with open("error_excel_sheets_to_csv.txt", "w") as f:
        for item in error_list:
            f.write(str(item) + "\n")   

schema_key = 1
schema = schema_utils.get_schema()
schema_new = {}
flles =  []
schema_type = []
result = {}
files_with_duplicate_columns = []
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

    new_key = f'UNknown_Schema_{schema_key}'
    schema_key += 1

    schema_new[new_key] = new_list    
    return new_key

def get_csv_headers(file_path):
    print(file_path)
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
    

def get_csv_schema(folder_path):
    """Returns a list of tuples containing the parent folder name and file name for all CSV and Excel files."""
    valid_extensions = (".csv")
    folder_path = Path(folder_path)

    for file in folder_path.rglob("*"):
        if file.is_file() and file.suffix.lower() in valid_extensions:  
            print(f"Generating schema for file : {str(file)}")
            csv_column_names, is_unique = get_csv_headers(file)
            
            if is_unique:
                get_schema = add_header_to_dict(csv_column_names)
                schema_type.append(get_schema)

                flles.append(file)
            else:
                files_with_duplicate_columns.append(file)
    
    result['file_name'] = flles
    result['schema_type'] = schema_type

    with open("csv_with_duplicate_columns.txt", "w") as f:
        for item in files_with_duplicate_columns:
            f.write(str(item) + "\n")   

    return result, schema_new



