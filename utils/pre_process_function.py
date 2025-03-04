import pandas as pd
import numpy as np
import uuid
import os
from pathlib import Path
import dateutil.parser as dparser
from tqdm import tqdm

def preprocess (file_path,schema_type,als_lab):
    func = lookup_preprocess_function.get(schema_type)  # Get function from dictionary

    if func: 
        try:
            df = pd.read_csv(file_path, low_memory=False, encoding="utf-8") # Try UTF-8 first
        except UnicodeDecodeError:
            print(f"⚠️ Encoding error in file: {file_path}. Retrying with Latin-1.")
            df = pd.read_csv(file_path, low_memory=False, encoding="latin-1")  # Use Latin-1 as fallback

        prep_df = func(df, als_lab) 
        func_name = func.__name__
        return prep_df, func_name
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
        try:
            prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])
        except:
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
    else:
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
    prep_data["ProductId"] = prep_data["ProductId"].astype(str)
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

    try:
        # Set order invoiced date to datetime format
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"])
    except:
        prep_data["order_invoiced_date"] = pd.to_datetime(prep_data["order_invoiced_date"], format="%d/%m/%Y")
        


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

def preprocess_leca_greatlab(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
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
        "Client",
        "Prac",
        "Acct",
        "Item",
        "SKU",
        "CAT",
        "SubCat",
        "Mat",
        "Stan",
        "Product count",
        "Revenue",
        "Tax",
        "Total"
    ]

    # Rename columns
    prep_data = prep_data.rename(
        columns={            
            "Prac": "practice_name",
            "Acct": "customer_id",
            "Client": "customer_name",
            "SKU": "product_code",
            "Item": "product_description",            
            "Product count": "quantity",
            "Revenue":"net_sales"
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
    prep_data["system_source"] = "Great Lab"
    prep_data["als_lab"] = als_lab
    
    prep_data["nhs_or_private"] = ""
    prep_data["practice_code"] = ""

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

def preprocess_leca_transactor(raw_data: pd.DataFrame, als_lab: str) -> pd.DataFrame:
    """

    :param raw_data:
    :param als_lab:
    :return:
    """
    prep_data = raw_data.copy(deep=True)

    # prep_data.columns = [
    #     "Practice",
    #     "Invoice.Date",
    #     "Invoice.AccountName",
    #     "Prac",
    #     "Acct",
    #     "Item",
    #     "SKU",
    #     "CAT",
    #     "SubCat",
    #     "Mat",
    #     "Stan",
    #     "Product count",
    #     "Revenue",
    #     "Tax",
    #     "Total"
    # ]

    # Rename columns
    prep_data = prep_data.rename(
        columns={            
            "Practice": "practice_name",
            "Invoice.AccountReference": "customer_id",
            "Invoice.AccountName": "customer_name",
            "InvoiceItem.ProductAccountReference2": "product_code",
            "Product": "product_description",            
            "InvoiceItem.Quantity": "quantity",
            "InvoiceItem.AmountNet":"net_sales"
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
    prep_data["year_month"] = pd.to_datetime(prep_data["Invoice.Date"])
    
    # Add labels for raw data system source and dental lab source
    prep_data["system_source"] = "Leca"
    prep_data["als_lab"] = als_lab
    

    prep_data["nhs_or_private"] = ""
    prep_data["practice_code"] = ""

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
        "Schema_11":preprocess_leca_greatlab,
        "Schema_12":preprocess_leca_transactor,
}

def preprocess_labtrac_ashford():
    """
    Input and preprocess raw sales data from the Ashford lab, extracted from the Labtrac system.
    :return prep_data: DataFrame containing pre-processed data for Ashford, from Labtrac.
    """
    ashford_2023 = pd.read_excel("data\sales_ashford\Ashford\Ashford 2023 Labtrac Data.xlsx")
    ashford_2022 = pd.read_excel("data\sales_ashford\Ashford\Ashford 2022 Labtrac Data.xlsx")
    ashford_2021 = pd.read_excel("data\sales_ashford\Ashford\Ashford 2021 Labtrac Data.xlsx")
    try:
        ashford_2023_Nov_Dec = pd.read_csv("data\sales_ashford\Ashford\Ashford 2023_Nov_Dec Labtrac Data.csv", encoding="utf-8") 
    except:
        ashford_2023_Nov_Dec = pd.read_csv("data\sales_ashford\Ashford\Ashford 2023_Nov_Dec Labtrac Data.csv", encoding="latin-1")
    
    ashford_2024_Jan_Apr = pd.read_excel("data\sales_ashford\Ashford\Ashford Data 2024_Jan_Apr Labtrac Data.xlsx")

    def set_new_columns(df, new_columns):
        """
        For input df, takes column headers and inserts them into the dataframe as a new row, then replaces the old
        column headers with new column headers of user's choosing.
        """
        print("Setting Columns")
        df.loc[-1] = df.columns
        df.index = df.index + 1
        df = df.sort_index()
        df.columns = new_columns

        return df
    
    def add_dr_id_column(df: pd.DataFrame):
        """
        For input df (2021 and 2022 data), adds a new DR ID column to match 2023 data.
        """
        df["DR ID"] = "Unknown"
        df_cols = df.columns.tolist()
        df_cols = df_cols[-1:] + df_cols[:-1]
        df = df[df_cols]
        
        return df

    # Combine the separate yearly files together
    new_columns = ashford_2023.columns
    prep_data = ashford_2023.copy(deep=True)

    prep_data["INVOICED DATE"] = prep_data["INVOICED DATE"].astype(str)
    prep_data = prep_data.loc[~prep_data["INVOICED DATE"].str.contains("1899")]
    prep_data["INVOICED DATE"] = prep_data["INVOICED DATE"].str.replace(".1", "")
    # prep_data = prep_data.loc[~(prep_data["INVOICED DATE"] == ".1")]
    prep_data["INVOICED DATE"] = pd.to_datetime(prep_data["INVOICED DATE"])

    prep_data = prep_data.loc[prep_data["INVOICED DATE"] < "2023-11-1"]

    ashford_2022 = add_dr_id_column(ashford_2022)
    
    for data in [ashford_2022, ashford_2021, ashford_2024_Jan_Apr]:
        data = data.reset_index(drop=True)
        data = set_new_columns(df=data, new_columns=new_columns)
        print(prep_data.shape)
        # data.to_csv(r"C:\Users\ZhiningLiu\OneDrive - Ansor\Desktop\data_0.csv")
        prep_data = pd.concat([prep_data, data]).reset_index(drop=True)
        print(prep_data.shape)
    # prep_data.to_csv(r"C:\Users\ZhiningLiu\OneDrive - Ansor\Desktop\prep_data_0.csv")

    ashford_2023_Nov_Dec = ashford_2023_Nov_Dec.reset_index(drop=True)
    ashford_2023_Nov_Dec = set_new_columns(df=ashford_2023_Nov_Dec, new_columns=new_columns)
    ashford_2023_Nov_Dec["INVOICED DATE"] = ashford_2023_Nov_Dec["INVOICED DATE"].str.rstrip(".1")
    ashford_2023_Nov_Dec["INVOICED DATE"] = pd.to_datetime(
        ashford_2023_Nov_Dec["INVOICED DATE"], format="%d/%m/%Y", dayfirst=True
    )
    ashford_2023_Nov_Dec = ashford_2023_Nov_Dec.loc[ashford_2023_Nov_Dec["INVOICED DATE"] > "2023-10-31"]
    prep_data = pd.concat([prep_data, ashford_2023_Nov_Dec]).reset_index(drop=True)

    # prep_data.to_csv(r"C:\Users\ZhiningLiu\OneDrive - Ansor\Desktop\prep_data-1.csv")

    # Fill in null values in the product description column
    prep_data["PRODUCT DESC"] = prep_data["PRODUCT DESC"].fillna(
        "No product description"
    )

    # Set the column data types
    prep_data["DR ID"] = prep_data["DR ID"].astype(str)
    prep_data["DOCTOR NAME"] = prep_data["DOCTOR NAME"].astype(str)
    prep_data["PRACTICE"] = prep_data["PRACTICE"].astype(str)
    prep_data["CASE NUMBER"] = prep_data["CASE NUMBER"].astype(str)
    prep_data["STATUS"] = prep_data["STATUS"].astype(str)
    # prep_data["DATE IN"] = pd.to_datetime(
    #     prep_data["DATE IN"], format="%d/%m/%Y"
    # )
    # prep_data["DUE DATE"] = pd.to_datetime(
    #     prep_data["DUE DATE"], format="%d/%m/%Y"
    # )
    # prep_data.to_csv(r"C:\Users\ZhiningLiu\OneDrive - Ansor\Desktop\prep_data-2.csv")
    prep_data["INVOICED DATE"] = prep_data["INVOICED DATE"].astype(str)
    prep_data = prep_data.loc[~prep_data["INVOICED DATE"].str.contains("1899")]
    prep_data["INVOICED DATE"] = prep_data["INVOICED DATE"].str.replace(".1", "")
    # prep_data = prep_data.loc[~(prep_data["INVOICED DATE"] == ".1")]
    prep_data["INVOICED DATE"] = pd.to_datetime(prep_data["INVOICED DATE"])
    # prep_data["INVOICED DATE"] = prep_data["INVOICED DATE"].str.rstrip(".1")
    # prep_data = prep_data.loc[~prep_data["INVOICED DATE"].str.contains("1899")]

    # prep_data["INVOICED DATE"] = pd.to_datetime(
    #     prep_data["INVOICED DATE"], format="%d/%m/%Y"
    # )
    prep_data["PRODUCT ID"] = prep_data["PRODUCT ID"].astype(str)
    prep_data["PRODUCT DESC"] = prep_data["PRODUCT DESC"].astype(str)
    prep_data["UNIT"] = prep_data["UNIT"].astype(int)
    prep_data["PRICE"] = prep_data["PRICE"].astype(str)
    prep_data["PRICE"] = prep_data["PRICE"].str.replace("£", "")
    prep_data["PRICE"] = prep_data["PRICE"].str.replace("Â", "")
    prep_data["PRICE"] = prep_data["PRICE"].str.replace(",", "")
    prep_data["PRICE"] = prep_data["PRICE"].astype(float)

    # Add unique row identifier
    prep_data["order_uuid"] = prep_data.apply(lambda _: uuid.uuid4(), axis=1)
    prep_data["order_uuid"] = prep_data["order_uuid"].astype(str)

    # Remove any rows with dates prior to 2021
    # prep_data = prep_data.loc[prep_data["INVOICED DATE"] > pd.Timestamp(2021, 1, 1)]

    # Add ALS lab identifier and system source
    prep_data["als_lab"] = "Ashford"
    prep_data["system_source"] = "Labtrac"

    # Remove any trailing spaces from the product codes
    prep_data["PRODUCT ID"] = prep_data["PRODUCT ID"].str.strip()

    # Rename and reorder columns
    prep_data = prep_data.rename(
        columns={
            "DR ID": "customer_id",
            "DOCTOR NAME": "customer_name",
            "PRACTICE": "practice_name",
            "CASE NUMBER": "order_id",
            "STATUS": "order_status",
            "DUE DATE": "order_due_date",
            "DATE IN": "order_created_date",
            "INVOICED DATE": "order_invoiced_date",
            "PRODUCT ID": "product_code",
            "PRODUCT DESC": "product_description",
            "UNIT": "quantity",
            "PRICE": "net_sales",
        }
    )
    prep_data = prep_data[
        [
            "order_uuid",
            "order_invoiced_date",
            "system_source",
            "als_lab",
            "practice_name",
            "customer_id",
            "customer_name",
            "product_code",
            "product_description",
            "quantity",
            "net_sales",
        ]
    ]

    # Find the unit net price for each order
    prep_data["unit_net_price"] = prep_data["net_sales"] / prep_data["quantity"]

    return prep_data

def preprocess_evident_densign(als_lab: str) -> pd.DataFrame:
    """
    Input and preprocess raw data from a single lab from the Evident system.
    :param als_lab: Str name of the ALS dental lab the data is from.
    :return prep_data: DataFrame containing preprocessed data for a single lab from Evident.
    """
    # Find all filepaths of data files for the given lab using Evident
    folder_path = "data\sales_densign\densign"
    file_path_list = [
        os.path.join(folder_path, file_path) for file_path in os.listdir(folder_path)
    ]

    # Download and clean and join the monthly reports together
    df_list = []
    for file_path in tqdm(file_path_list):
        # Load in the data for the month
        df = pd.read_excel(file_path)

        # Extract the month that the data is from
        date_str = df["Densign Lab"].values[1]
        first_date = dparser.parse(date_str.split("-")[0], fuzzy=True)

        # Remove extraneous rows at the top of the original Excel file and set the correct column headers
        df = df.iloc[5:, :].reset_index(drop=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:, :].reset_index(drop=True)

        # Fill in the missing values in the first three columns, to convert from Excel template format to data table
        if (
            "Customer Code" in df.columns
            and "Customer Name" not in df.columns
            and "Group" not in df.columns
        ):
            df[["Customer Code", "Dentist Name", "Practice Name"]] = df[
                ["Customer Code", "Dentist Name", "Practice Name"]
            ].ffill(axis=0)
        elif (
            "Customer Name" in df.columns
            and "Customer Code" not in df.columns
            and "Group" not in df.columns
        ):
            df[["Customer Name", "Dentist Name", "Practice Name"]] = df[
                ["Customer Name", "Dentist Name", "Practice Name"]
            ].ffill(axis=0)
            df = df.rename(columns={"Customer Name": "Customer Code"})
        elif (
            "Group" in df.columns
            and "Customer Code" not in df.columns
            and "Customer Name" not in df.columns
        ):
            df[["Group", "Dentist Name", "Practice Name"]] = df[
                ["Group", "Dentist Name", "Practice Name"]
            ].ffill(axis=0)
            df = df.rename(columns={"Group": "Customer Code"})

        df = df.dropna(axis=0, how="any", subset="Item")

        # Add date column
        df["year_month"] = first_date

        # Drop the % columns
        df = df.drop(
            columns=[
                "%",
                "Alloy",
                "COGS Alloy",
                "Tax",
            ]
        )

        # Ensure all rows are correctly ordered
        df = df[
            [
                "Customer Code",
                "Dentist Name",
                "Practice Name",
                "Item",
                "Product Pieces",
                "Remake Pieces",
                "Revenue",
                "Total",
                "year_month",
            ]
        ]

        # Add to the list of cleaned dfs, ready to be joined together after all monthly data files have been cleaned
        df_list.append(df)

    prep_data = pd.concat(df_list)

    # Remove any trailing spaces from the product codes
    prep_data["Item"] = prep_data["Item"].str.strip()

    # Rename columns
    prep_data = prep_data.rename(
        columns={
            "Customer Code": "customer_id",
            "Dentist Name": "customer_name",
            "Practice Name": "practice_name",
            "Item": "product_description",
            "Product Pieces": "quantity",
            "Remake Pieces": "quantity_remake",
            "Revenue": "net_sales",
            "Total": "gross_sales",
        }
    )

    # Add system source and ALS lab columns
    prep_data["system_source"] = "Evident"
    prep_data["als_lab"] = als_lab

    # Add unique row identifier
    prep_data["customer_product_cube_uuid"] = prep_data.apply(
        lambda _: uuid.uuid4(), axis=1
    )
    prep_data["customer_product_cube_uuid"] = prep_data[
        "customer_product_cube_uuid"
    ].astype(str)

    # Fill the product id column
    prep_data["product_id"] = prep_data["product_description"]

    # Reorder columns
    prep_data = prep_data[
        [
            "customer_product_cube_uuid",
            "year_month",
            "system_source",
            "als_lab",
            "practice_name",
            "customer_id",
            "customer_name",
            "product_id",
            "product_description",
            "quantity",
            "quantity_remake",
            "net_sales",
            "gross_sales",
        ]
    ]

    return prep_data
