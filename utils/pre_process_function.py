import pandas as pd
import numpy as np
import uuid

def preprocess (file_path,schema_type,als_lab):
    func = lookup_preprocess_function.get(schema_type)  # Get function from dictionary

    if func: 
        try:
            df = pd.read_csv(file_path, low_memory=False, encoding="utf-8") # Try UTF-8 first
        except UnicodeDecodeError:
            print(f"⚠️ Encoding error in file: {file_path}. Retrying with Latin-1.")
            df = pd.read_csv(file_path, low_memory=False, encoding="latin-1")  # Use Latin-1 as fallback

        prep_df = func(df, als_lab) 
        return prep_df 
    else:
        print(f"No schema found for file {file_path}")          


def preprocess_labtrac_new(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
    """
    Preprocess raw data from a single lab from the Labtrac system in the full 2021 to 2023 format.
    :param raw_data: DataFrame containing the raw data for a single lab from Labtrac.
    :param als_lab: Str name of the ALS dental lab the data is from.
    :return prep_data: DataFrame containing preprocessed data for a single lab from Labtrac.
    """
    prep_data = raw_data.copy(deep=True)

    cols_to_cut = [
        "Order",
        "Stage",
        "Patient",
        "Recieved",
        "Due Date",
        "DoctorAnalysis1",
        "DoctorAnalysis2",
        "DoctorAnalysis3",
        "DoctorAnalysis4",
        "DoctorAnalysis5",
        "Invoice 1",
        "Invoice 2",
        "Invoice 3",
        "Invoice 4",
        "Invoice 5",
        "Delivery 4",
        "Status",
    ]

    # Drop extraneous columns
    for col in prep_data.columns:
        if col in cols_to_cut:
            prep_data = prep_data.drop(columns=col)
        if col == "Date":
            prep_data = prep_data.rename(columns={"Date": "CompletedDate"})

    # Drop rows where the invoice has not been sent, as these orders will not yet appear in accounting revenue
    prep_data = prep_data.dropna(subset="CompletedDate", axis=0, how="all").reset_index(
        drop=True
    )

    # Remove any trailing spaces from the product codes
    prep_data["Description"] = prep_data["Description"].str.strip()

    # Rename columns
    prep_data = prep_data.rename(
        columns={
            "Code": "customer_id",
            "Name": "customer_name",
            "Delivery 1": "practice_name",
            "Delivery 2": "practice_address_road",
            "Delivery 3": "practice_address_town",
            "Delivery 5": "practice_address_postcode",
            "CompletedDate": "order_invoiced_date",
            "Product Link": "product_code",
            "Description": "product_description",
            "Value": "net_sales",
            "Qty": "quantity",
            "Category": "product_category",
            "Standard": "nhs_or_private"
        }
    )

    # Set order invoiced date to datetime format
    if als_lab == "Romak Denture Centre":
        prep_data = prep_data.loc[prep_data["order_invoiced_date"] != "00/01/1900", :].reset_index(drop=True)
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"], format="%d/%m/%Y")
    elif als_lab == "APlus":
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])
    elif als_lab == "Central Dental Laboratory":
        prep_data["order_invoiced_date"] = prep_data["order_invoiced_date"].astype(str)
        prep_data = prep_data.loc[prep_data["order_invoiced_date"] != "00:00:00", :].reset_index(drop=True)
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])
    elif als_lab == "Lodge":
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])
    elif als_lab == "Precedental":
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])

    # Drop the product_category column if it is empty
    if prep_data[["product_category"]].isnull().any().any() == True:
        prep_data = prep_data.drop(columns="product_category")

    # Remove trailing spaces string columns
    for col in ["product_code", "customer_name", "product_description", "product_category", "nhs_or_private"]:
        if col in prep_data.columns:
            prep_data[col] = prep_data[col].str.strip()

    # Add system source and ALS lab columns
    prep_data["system_source"] = "Labtrac"
    prep_data["als_lab"] = als_lab

    # Add unique row identifier
    prep_data["order_uuid"] = prep_data.apply(lambda _: uuid.uuid4(), axis=1)
    prep_data["order_uuid"] = prep_data["order_uuid"].astype(str)

    if "product_category" in prep_data.columns:
        prep_data = prep_data[
            [
                "order_uuid",
                "order_invoiced_date",
                "system_source",
                "als_lab",
                "practice_name",
                "practice_address_road",
                "practice_address_town",
                "practice_address_postcode",
                "customer_id",
                "customer_name",
                "product_code",
                "product_description",
                "product_category",
                "quantity",
                "net_sales",
                "nhs_or_private"
            ]
        ]
    else:
        prep_data = prep_data[
            [
                "order_uuid",
                "order_invoiced_date",
                "system_source",
                "als_lab",
                "practice_name",
                "practice_address_road",
                "practice_address_town",
                "practice_address_postcode",
                "customer_id",
                "customer_name",
                "product_code",
                "product_description",
                "quantity",
                "net_sales",
                "nhs_or_private"
            ]
        ]

    # Find the unit net price for each order
    prep_data["unit_net_price"] = prep_data["net_sales"] / prep_data["quantity"]

    return prep_data

def preprocess_labtrac_old(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
    """
    Preprocess raw data from a single lab from the Labtrac system in the old 2021 to Oct 2023 format.
    :param raw_data: DataFrame containing the raw data for a single lab from Labtrac.
    :param als_lab: Str name of the ALS dental lab the data is from.
    :return prep_data: DataFrame containing preprocessed data for a single lab from Labtrac.
    """
    prep_data = raw_data.copy(deep=True)

    # Drop extraneous columns
    prep_data = prep_data.drop(
        columns=[
            "LicenceNo",
            "DoctorAnalysis1",
            "DoctorAnalysis2",
            "DoctorAnalysis3",
            "DoctorAnalysis4",
            "DoctorAnalysis5",
            "DoctorAnalysis6",
            "DoctorAnalysis7",
            "DoctorAnalysis8",
            "DoctorAnalysis9",
            "DoctorAnalysis10",
            "ProductAnalysis1",
            "ProductAnalysis2",
            "ProductAnalysis3",
            "ProductAnalysis4",
            "ProductAnalysis5",
            "ProductAnalysis6",
            "ProductAnalysis7",
            "ProductAnalysis8",
            "ProductAnalysis9",
            "ProductAnalysis10",
            "MaterialId",
            "MaterialName",
            "MaterialAnalysis1",
            "MaterialAnalysis2",
            "MaterialAnalysis3",
            "MaterialAnalysis4",
            "MaterialAnalysis5",
            "MaterialAnalysis6",
            "MaterialAnalysis7",
            "MaterialAnalysis8",
            "MaterialAnalysis9",
            "MaterialAnalysis10",
        ]
    )

    # Drop rows where the invoice has not been sent, as these orders will not yet appear in accounting revenue
    prep_data = prep_data.dropna(subset="InvoiceDate", axis=0, how="all").reset_index(
        drop=True
    )

    # # Drop rows where the sales value was zero (primarily shipping)
    # prep_data = prep_data.loc[prep_data["Net"] != 0, :]

    # Remove any trailing spaces from the product codes
    prep_data["ProductId"] = prep_data["ProductId"].str.strip()

    # Rename columns
    prep_data = prep_data.rename(
        columns={
            "DoctorId": "customer_id",
            "DoctorName": "customer_name",
            "Address1": "practice_name",
            "Address2": "practice_address_road",
            "Address3": "practice_address_town",
            "Address5": "practice_address_postcode",
            "InvoiceDate": "order_invoiced_date",
            "ProductId": "product_code",
            "ProductName": "product_description",
            "Net": "net_sales",
            "Qty": "quantity",
            "Status": "order_status",
            "CurrencySymbol": "currency_symbol",
            "CurrencyDescription": "currency_description",
        }
    )

    # Set order invoiced date to datetime format
    prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])

    # Add system source and ALS lab columns
    prep_data["system_source"] = "Labtrac"
    prep_data["als_lab"] = als_lab

    # Add unique row identifier
    prep_data["order_uuid"] = prep_data.apply(lambda _: uuid.uuid4(), axis=1)
    prep_data["order_uuid"] = prep_data["order_uuid"].astype(str)

    prep_data = prep_data[
        [
            "order_uuid",
            "order_invoiced_date",
            "system_source",
            "als_lab",
            "practice_name",
            "practice_address_road",
            "practice_address_town",
            "practice_address_postcode",
            "customer_id",
            "customer_name",
            "product_code",
            "product_description",
            "quantity",
            "net_sales",
            "order_status",
            "currency_symbol",
            "currency_description",
        ]
    ]

    # Find the unit net price for each order
    prep_data["unit_net_price"] = prep_data["net_sales"] / prep_data["quantity"]

    return prep_data

def preprocess_transactor(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
    """
    Preprocess raw data from a single lab from the Transactor system.
    :param raw_data: DataFrame containing the raw data for a single lab from Transactor.
    :param als_lab: Str name of the ALS dental lab the data is from.
    :return prep_data: DataFrame containing preprocessed data for a single lab from Transactor.
    """
    prep_data = raw_data.copy(deep=True)

    #  Ensure the correct number of columns, and that column names are correct
    if prep_data.shape[1] != 15:
        raise Exception(
            "Raw Transactor data does not have the correct number of columns."
        )

    # Check sums for quantity and sales match totals in the raw data, then drop the report total row
    for col in ["Qty", "Net_Sales", "Tax_Sales"]:
        if (
            prep_data.loc[prep_data["Description"] != "REPORT TOTALS:", col].sum()
            - prep_data.loc[prep_data["Description"] == "REPORT TOTALS:", col].values[0]
        ) > 0.01:
            raise Exception(
                f"Total {col} from Transactor data does not match report total in original raw data."
            )

    prep_data = prep_data.loc[prep_data["Description"] != "REPORT TOTALS:", :].copy(
        deep=True
    )

    # Fill in null values in ShipFullName column
    prep_data["ShipFullName"] = prep_data["ShipFullName"].fillna("Unknown")
    prep_data["ShipAddress"] = prep_data["ShipAddress"].fillna("Unknown")
    prep_data["CustFullName"] = prep_data["CustFullName"].fillna("Unknown")
    prep_data["ShipID"] = prep_data["ShipID"].fillna(0)

    # Set the column data types
    prep_data["Year"] = prep_data["Year"].astype(int)
    prep_data["Month"] = prep_data["Month"].astype(int)
    prep_data["ShipID"] = prep_data["ShipID"].astype(int)
    prep_data["ShipFullName"] = prep_data["ShipFullName"].astype(str)
    prep_data["ShipAddress"] = prep_data["ShipAddress"].astype(str)
    prep_data["CustID"] = prep_data["CustID"].astype(int)
    prep_data["CustFullName"] = prep_data["CustFullName"].astype(str)
    prep_data["code"] = prep_data["code"].astype(str)
    prep_data["Description"] = prep_data["Description"].astype(str)
    prep_data["PriceBand"] = prep_data["PriceBand"].astype(str)
    prep_data["NetUnitPrice"] = prep_data["NetUnitPrice"].astype(float)
    prep_data["DiscountedUnitPrice"] = prep_data["DiscountedUnitPrice"].astype(float)
    prep_data["Qty"] = prep_data["Qty"].astype(float)
    prep_data["Net_Sales"] = prep_data["Net_Sales"].astype(float)
    prep_data["Tax_Sales"] = prep_data["Tax_Sales"].astype(float)

    # Add unique row identifier
    prep_data["customer_product_cube_uuid"] = prep_data.apply(
        lambda _: uuid.uuid4(), axis=1
    )
    prep_data["customer_product_cube_uuid"] = prep_data[
        "customer_product_cube_uuid"
    ].astype(str)

    # Combine year and month into single datetime column
    prep_data["year_month"] = pd.to_datetime(prep_data[["Year", "Month"]].assign(DAY=1))

    # Add labels for raw data system source and dental lab source
    prep_data["system_source"] = "Transactor"
    prep_data["als_lab"] = als_lab

    # Drop extraneous columns
    prep_data = prep_data.drop(["Year", "Month"], axis=1)

    # Remove any trailing spaces from the product codes
    prep_data["code"] = prep_data["code"].str.strip()

    # Rename and reorder columns
    prep_data = prep_data.rename(
        columns={
            "ShipID": "ship_id",
            "ShipFullName": "ship_name",
            "ShipAddress": "ship_address",
            "CustID": "customer_id",
            "CustFullName": "customer_name",
            "code": "product_code",
            "Description": "product_description",
            "PriceBand": "original_lab_price_band",
            "NetUnitPrice": "net_unit_price",
            "DiscountedUnitPrice": "discounted_unit_price",
            "Qty": "quantity",
            "Net_Sales": "net_sales",
            "Tax_Sales": "tax_sales",
        }
    )
    prep_data = prep_data[
        [
            "customer_product_cube_uuid",
            "year_month",
            "system_source",
            "als_lab",
            "ship_id",
            "ship_name",
            "ship_address",
            "customer_id",
            "customer_name",
            "product_code",
            "product_description",
            "original_lab_price_band",
            "net_unit_price",
            "discounted_unit_price",
            "quantity",
            "net_sales",
            "tax_sales",
        ]
    ]

    return prep_data

def preprocess_leca(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
    """

    :param raw_data:
    :param als_lab:
    :return:
    """
    prep_data = raw_data.copy(deep=True)

    # Replace unnamed columns with first row of data table if relevant
    if "Unnamed: 0" in prep_data.columns:
        prep_data.columns = prep_data.iloc[0]
        prep_data = prep_data.iloc[1:].reset_index(drop=True)

    prep_data.columns = [
        "Year",
        "Month",
        "Practice Post Code or Identifier",
        "Practice Name",
        "Customer Account",
        "Dentist",
        "Product Code",
        "Product Description",
        "NHS /Private/Independent/PPE",
        "Quantity",
        "Invoice Amount",
        "Invoice VAT",
    ]

    # Rename columns
    prep_data = prep_data.rename(
        columns={
            "Practice Post Code or Identifier": "practice_code",
            "Practice Name": "practice_name",
            "Customer Account": "customer_id",
            "Dentist": "customer_name",
            "Product Code": "product_code",
            "Product Description": "product_description",
            "NHS /Private/Independent/PPE": "nhs_or_private",
            "Quantity": "quantity"
        }
    )

    # Add unique row identifier
    prep_data["customer_product_cube_uuid"] = prep_data.apply(
        lambda _: uuid.uuid4(), axis=1
    )
    prep_data["customer_product_cube_uuid"] = prep_data[
        "customer_product_cube_uuid"
    ].astype(str)

    # Combine year and month into single datetime column
    prep_data["year_month"] = pd.to_datetime(prep_data[["Year", "Month"]].assign(DAY=1))

    # Add labels for raw data system source and dental lab source
    prep_data["system_source"] = "Custom"
    prep_data["als_lab"] = als_lab

    # Create net sales column by minusing VAT from the invoice value
    prep_data["net_sales"] = prep_data["Invoice Amount"] - prep_data["Invoice VAT"]

    # Drop extraneous columns and reorder columns
    prep_data = prep_data[
        [
            "customer_product_cube_uuid",
            "year_month",
            "system_source",
            "als_lab",
            "practice_code",
            "practice_name",
            "customer_id",
            "customer_name",
            "product_code",
            "product_description",
            "quantity",
            "net_sales",
            "nhs_or_private"
        ]
    ]

    # Add columns for the net unit price
    prep_data["net_unit_price"] = prep_data["net_sales"] / prep_data["quantity"]

    return prep_data

def prep_transactor_passion_dental_design(raw_data: pd.DataFrame, als_lab: str):
    """
    Preprocess raw data from Passion Dental Design lab from the Transactor system.
    :param raw_data: DataFrame containing the raw data for Passion Dental Design lab from Transactor.
    :param als_lab: Str name of Passion Dental Design.
    :return prep_data: DataFrame containing preprocessed data for Passion Dental Design lab from Transactor.
    """
    prep_data = raw_data.copy(deep=True)

    #  Ensure the correct number of columns, and that column names are correct
    if prep_data.shape[1] != 11:
        raise Exception(
            "Raw Transactor data does not have the correct number of columns."
        )

    # Check sums for quantity and sales match totals in the raw data, then drop the report total row
    for col in ["Qty", "Net_Sales", "Tax_Sales"]:
        if (
            prep_data.loc[prep_data["Description"] != "REPORT TOTALS:", col].sum()
            - prep_data.loc[prep_data["Description"] == "REPORT TOTALS:", col].values[0]
        ) > 0.01:
            raise Exception(
                f"Total {col} from Transactor data does not match report total in original raw data."
            )
    prep_data = prep_data.loc[prep_data["Description"] != "REPORT TOTALS:", :].copy(
        deep=True
    )

    # Fill in null values in ShipFullName column
    prep_data["shipfullname"] = prep_data["shipfullname"].fillna("Unknown")
    prep_data["custID"] = prep_data["custID"].fillna("Unknown")
    prep_data["CustFullName"] = prep_data["CustFullName"].fillna("Unknown")
    prep_data["shipid"] = prep_data["shipid"].fillna(0)

    # Set the column data types
    prep_data["Year"] = prep_data["Year"].astype(int)
    prep_data["Month"] = prep_data["Month"].astype(int)
    prep_data["shipid"] = prep_data["shipid"].astype(int)
    prep_data["shipfullname"] = prep_data["shipfullname"].astype(str)
    prep_data["custID"] = prep_data["custID"].astype(int)
    prep_data["CustFullName"] = prep_data["CustFullName"].astype(str)
    prep_data["code"] = prep_data["code"].astype(str)
    prep_data["Description"] = prep_data["Description"].astype(str)
    prep_data["Qty"] = prep_data["Qty"].astype(float)
    prep_data["Net_Sales"] = prep_data["Net_Sales"].astype(float)
    prep_data["Tax_Sales"] = prep_data["Tax_Sales"].astype(float)

    # Add unique row identifier
    prep_data["customer_product_cube_uuid"] = prep_data.apply(
        lambda _: uuid.uuid4(), axis=1
    )
    prep_data["customer_product_cube_uuid"] = prep_data[
        "customer_product_cube_uuid"
    ].astype(str)

    # Combine year and month into single datetime column
    prep_data["year_month"] = pd.to_datetime(prep_data[["Year", "Month"]].assign(DAY=1))

    # Add labels for raw data system source and dental lab source
    prep_data["system_source"] = "Transactor"
    prep_data["als_lab"] = als_lab

    # Drop extraneous columns
    prep_data = prep_data.drop(["Year", "Month"], axis=1)

    # Remove any trailing spaces from the product codes
    prep_data["code"] = prep_data["code"].str.strip()

    # Add additional empty columns to match what is seen in other labs' Transactor reports
    for col in ["ship_address", "original_lab_price_band", "net_unit_price", "discounted_unit_price"]:
        if col in ["ship_address", "original_lab_price_band"]:
            prep_data[col] = "Unknown"
        elif col in ["net_unit_price", "discounted_unit_price"]:
            prep_data[col] = np.nan

    # Rename and reorder columns
    prep_data = prep_data.rename(
        columns={
            "shipid": "ship_id",
            "shipfullname": "ship_name",
            "custID": "customer_id",
            "CustFullName": "customer_name",
            "code": "product_code",
            "Description": "product_description",
            "Qty": "quantity",
            "Net_Sales": "net_sales",
            "Tax_Sales": "tax_sales",
        }
    )
    prep_data = prep_data[
        [
            "customer_product_cube_uuid",
            "year_month",
            "system_source",
            "als_lab",
            "ship_id",
            "ship_name",
            "ship_address",
            "customer_id",
            "customer_name",
            "product_code",
            "product_description",
            "original_lab_price_band",
            "net_unit_price",
            "discounted_unit_price",
            "quantity",
            "net_sales",
            "tax_sales",
        ]
    ]

    return prep_data


lookup_preprocess_function  = {
        "Schema_1":preprocess_labtrac_new,
        "Schema_2":preprocess_labtrac_new,
        "Schema_3":preprocess_labtrac_old,
        "Schema_4":preprocess_labtrac_old,
        "Schema_5":preprocess_transactor,
        "Schema_6":preprocess_transactor,
        "Schema_7":preprocess_leca,
        "Schema_8":preprocess_leca,
        "Schema_9":preprocess_leca,
        "Schema_10":prep_transactor_passion_dental_design,
        # "Schema_11":input_preprocess_evident,
        # "Schema_12":input_preprocess_labtrac_ashford,
}