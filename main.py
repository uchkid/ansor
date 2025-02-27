from utils import utils, pre_process
from utils import pre_process
import pandas as pd

#utils.get_excel_files()

df = utils.get_csv_schema()


print("Starting preprocessing of files")
df = pd.read_csv('files/file_with_schema.csv')
pre_process.preprocess(df)

