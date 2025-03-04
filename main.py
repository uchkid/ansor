from utils import utils, pre_process

utils.get_excel_files()

df = utils.get_csv_schema()


print("Starting preprocessing of files")
pre_process.preprocess(df)

print("Starting preprocessing of Ashford files")
pre_process.preprocess_ashford()

print("Starting preprocessing of Densign files")
pre_process.preprocess_densign()

print("Starting combining of files")
pre_process.combine_preprocess()

