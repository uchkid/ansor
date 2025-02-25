import os
from dotenv import load_dotenv
from utils.utils import *
from utils import schema_utils

load_dotenv()
folder_path = os.environ.get('RAW_FOLDER_PATH')

get_excel_files(folder_path)

result_dic, schema_dic = get_csv_schema(folder_path)
df = pd.DataFrame(result_dic)
df.to_csv("all-files-schema-attached.csv", index=False) 

with open("schema.txt", "w") as file:
    for key, value in schema_dic.items():
        file.write(f"{key}: {value}\n")