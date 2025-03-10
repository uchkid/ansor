from utils import utils, pre_process

# utils.get_excel_files()

# df = utils.get_csv_schema()


# print("Starting preprocessing of files")
# pre_process.preprocess(df)

# print("Starting preprocessing of Ashford files")
# pre_process.preprocess_ashford()

# print("Starting preprocessing of Densign files")
# pre_process.preprocess_densign()

# print("Starting combining of files")
# pre_process.combine_preprocess()

########################################################################################################################
# NHS mapping process start ####
########################################################################################################################
from pathlib import Path
from utils import mysettings, nhs_mapping

nhs_mapping_folder = Path(mysettings.NHS_MAPPING_FOLDER)
nhs_mapping_folder.mkdir(parents=True, exist_ok=True)  

print("Generating NHS mapping data for aesthetic_world")
aesthetic_world_nhs_codes = mysettings.AESTHETIC_WORLD_NHS_CODE_DATA
aesthetic_world_private_codes = mysettings.AESTHETIC_WORLD_PRIVATE_CODE_DATA
df = nhs_mapping.preprocess_aesthetic_world_nhs_private_mapping(aesthetic_world_nhs_codes,aesthetic_world_private_codes)
mapping_output_file = f"{nhs_mapping_folder}/aesthetic_world.csv" 
df.to_csv(mapping_output_file, index=False)

print("Generating NHS mapping data for woodford")
woodford_price_list = mysettings.WOODFORD_PRICE_LIST
df = nhs_mapping.preprocess_woodford_nhs_private_mapping(woodford_price_list)
mapping_output_file = f"{nhs_mapping_folder}/woodford.csv" 
df.to_csv(mapping_output_file, index=False)

ashford_price_list = mysettings.ASHFORD_PRICE_LIST