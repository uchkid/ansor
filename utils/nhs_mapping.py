import pandas as pd

def generate_aesthetic_world_nhs_private_mapping(
    aesthetic_world_nhs_codes, aesthetic_world_private_codes
):
    """
    Preprocess the NHS-private pricing lists for the Aesthetic World lab (Labtrac).
    :param aesthetic_world_nhs_codes: Raw list of NHS codes for Aesthetic World
    :param aesthetic_world_private_codes: Raw list of Private codes for Aesthetic World
    :return aesthetic_world_nhs_private_mapping: Preprocessed mapping of Aesthetic World products to NHS and Private
    tags.
    """
    
    nhs_codes = pd.read_excel(aesthetic_world_nhs_codes)
    private_codes = pd.read_excel(aesthetic_world_private_codes)

    df_list = []
    for df in [nhs_codes, private_codes]:
        df = df.drop(columns="Price1")
        df = df.rename(
            columns={
                "ProductID": "product_code",
                "Description": "product_description",
                "Standard": "nhs_or_private_mapping",
            }
        )
        df_list.append(df)

    aesthetic_world_nhs_private_mapping = pd.concat(df_list)

    aesthetic_world_nhs_private_mapping.loc[
        aesthetic_world_nhs_private_mapping["nhs_or_private_mapping"] == "Economy",
        "nhs_or_private_mapping",
    ] = "NHS"

    for col in aesthetic_world_nhs_private_mapping.columns:
        aesthetic_world_nhs_private_mapping[col] = aesthetic_world_nhs_private_mapping[col].str.strip()

    return aesthetic_world_nhs_private_mapping

def generate_woodford_nhs_private_mapping(
    woodford_price_list: pd.DataFrame
):
    """
    Preprocess the NHS-private pricing lists for the Woodford lab (Labtrac).
    :param aesthetic_world_nhs_codes: Raw list of NHS/Private codes for Woodford.
    :return woodford_nhs_private_mapping: Preprocessed mapping of Woodford products to NHS and Private
    tags.
    """
    prices = pd.read_excel(woodford_price_list)
    prices = prices.iloc[8:789, :]
    prices = prices[
        [
            "Product Price List 1 - Default",
            "Unnamed: 1",
            "Unnamed: 5"
        ]
    ].rename(
        columns={
            "Product Price List 1 - Default": "nhs_or_private_mapping",
            "Unnamed: 1": "product_code",
            "Unnamed: 5": "product_description"
        }
    ).reset_index(drop=True)

    prices = prices.dropna(subset=["product_code", "product_description"], how="all")

    for col in prices.columns:
        prices[col] = prices[col].str.strip()

    woodford_nhs_private_mapping = prices.copy(deep=True)

    return woodford_nhs_private_mapping

def nhs_private_tag_ashford(
    prep_data: pd.DataFrame, als_lab_postcodes_dict, ashford_price_list: pd.DataFrame
):
    """
    Tag sales in lab-level preprocessed sales data from Ashford to whether they are from NHS end-customer orders or
    private end-customer order.
    :param prep_data: Dataframe containing lab-level preprocessed sales data from Ashford.
    :param als_lab_postcodes_dict: Dict containing names and postcodes of ALS labs.
    :param ashford_price_list: Prices list separating NHS and private sales in Ashford data.
    :return tagged_data: DataFrame containing lab-level preprocessed data with sales tagged to NHS or private.
    """
    tagged_data = prep_data.copy(deep=True)

    # Unpack the ALS lab postcodes dict
    als_df = pd.DataFrame.from_dict(als_lab_postcodes_dict, orient="index").reset_index(
        drop=True
    )

    # Check if the ship name roughly matches an ALS lab name
    als_name_search_term_list = [
        "leca",
        "ashford",
        "casterbridge",
        "dental technique",
        "cardiff ortho",
        "woodford",
        "dental excellence",
        "passion",
        "ceramics",
        "denture centre",
        "veus",
        "precedental",
        "ken poland",
        "bristol crown",
        "bristol cadcam",
        "bristol cad-cam",
        "iw dental",
        "ip milling",
        "reiner",
        "halo",
        "burke ortho",
        "dent 8",
        "dent8",
        "densign",
        "aesthetic world",
        "aplus",
        "a plus",
        "oakview restorations",
        "oakview",
        "central dental",
        "central dental lab",
        "ceroplast",
        "european dental lab",
        "innovate",
    ]
    als_lab_data = tagged_data.copy(deep=True)
    als_lab_data["practice_name_lower_case"] = als_lab_data["practice_name"].str.lower()
    als_lab_data["customer_name_lower_case"] = als_lab_data["customer_name"].str.lower()
    df_list = []
    for substring in als_name_search_term_list:
        df_list.append(
            als_lab_data.loc[
                (
                    (als_lab_data["customer_name_lower_case"].str.contains(substring))
                    | (als_lab_data["practice_name_lower_case"].str.contains(substring))
                )
            ]
        )
    als_lab_data = pd.concat(df_list)

    # Tag rows with similar addresses and names as ALS lab as a lab-related sale
    als_lab_data["nhs_private_tag"] = "ALS Lab"
    als_lab_data = als_lab_data.reset_index(drop=True)
    tagged_data = tagged_data.merge(
        als_lab_data[["order_uuid", "nhs_private_tag"]], on="order_uuid", how="left"
    )

    # Add in NHS and private tags from the Ashford prices list
    ashford_price_list["ProductID"] = ashford_price_list["ProductID"].astype(str)
    tagged_data = tagged_data.merge(
        ashford_price_list, left_on="product_code", right_on="ProductID", how="left"
    )
    tagged_data["nhs_private_tag"] = tagged_data["nhs_private_tag"].fillna(
        tagged_data["Class"]
    )

    # Mark all RISIO sales as private
    tagged_data.loc[
        tagged_data["product_description"].str.contains("RISIO"), "nhs_private_tag"
    ] = "Private"

    tagged_data.loc[
        tagged_data["nhs_private_tag"].isna() == True, "nhs_private_tag"
    ] = "Unknown"

    # Drop extraneous columns
    tagged_data = tagged_data.drop(
        columns=["ProductID", "Description", "Price 2024", "Class"]
    )

    return tagged_data
