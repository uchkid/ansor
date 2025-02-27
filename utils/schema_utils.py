# lookup_preprocess_function  = {
#         "Schema_1":preprocess_labtrac_new,
#         "Schema_2":preprocess_labtrac_new,
#         "Schema_3":"preprocess_labtrac_old",
#         "Schema_4":"preprocess_labtrac_old",
#         "Schema_5":"preprocess_transactor",
#         "Schema_6":"preprocess_transactor",
#         "Schema_7":"preprocess_leca",
#         "Schema_8":"preprocess_leca",
#         "Schema_9":"preprocess_leca",
#         "Schema_10":"prep_transactor_passion_dental_design",
#         "Schema_11":"input_preprocess_evident",
#     }    

def lookup_schema():
    dic = {
        "Schema_1": ['Order', 'Patient', 'Code', 'Name', 'Recieved', 'Date', 'Product Link', 'Description', 'Value', 'Category', 'Standard', 'Status', 'Qty', 'Delivery 1', 'Delivery 2', 'Delivery 3', 'Delivery 4', 'Delivery 5', 'Invoice 1', 'Invoice 2', 'Invoice 3', 'Invoice 4', 'Invoice 5'],
        "Schema_2": ['Order', 'Stage', 'Patient', 'Code', 'Name', 'Recieved', 'Due Date', 'CompletedDate', 'Product Link', 'Description', 'Value', 'Qty', 'Category', 'Standard', 'Status', 'Delivery 1', 'Delivery 2', 'Delivery 3', 'Delivery 4', 'Delivery 5', 'Invoice 1', 'Invoice 2', 'Invoice 3', 'Invoice 4', 'Invoice 5', 'DoctorAnalysis1', 'DoctorAnalysis2', 'DoctorAnalysis3', 'DoctorAnalysis4', 'DoctorAnalysis5'],
        "Schema_3": ['Id', 'Reference', 'DoctorId', 'DoctorName', 'LicenceNo', 'Address1', 'Address2', 'Address3', 'Address4', 'Address5', 'DoctorAnalysis1', 'DoctorAnalysis2', 'DoctorAnalysis3', 'DoctorAnalysis4', 'DoctorAnalysis5', 'DoctorAnalysis6', 'DoctorAnalysis7', 'DoctorAnalysis8', 'DoctorAnalysis9', 'DoctorAnalysis10', 'DateCreated', 'DateDue', 'InvoiceDate', 'DateShipped', 'CategoryDescription', 'StandardDescription', 'ProductId', 'ProductName', 'ProductAnalysis1', 'ProductAnalysis2', 'ProductAnalysis3', 'ProductAnalysis4', 'ProductAnalysis5', 'ProductAnalysis6', 'ProductAnalysis7', 'ProductAnalysis8', 'ProductAnalysis9', 'ProductAnalysis10', 'MaterialId', 'MaterialName', 'MaterialAnalysis1', 'MaterialAnalysis2', 'MaterialAnalysis3', 'MaterialAnalysis4', 'MaterialAnalysis5', 'MaterialAnalysis6', 'MaterialAnalysis7', 'MaterialAnalysis8', 'MaterialAnalysis9', 'MaterialAnalysis10', 'Net', 'Tax', 'Gross', 'Units', 'Qty', 'Status', 'CurrencySymbol', 'CurrencyDescription'],
        "Schema_4": ['Id', 'Reference', 'DoctorId', 'DoctorName', 'LicenceNo', 'Address1', 'Address2', 'Address3', 'Address4', 'Address5', 'DoctorAnalysis1', 'DoctorAnalysis2', 'DoctorAnalysis3', 'DoctorAnalysis4', 'DoctorAnalysis5', 'DoctorAnalysis6', 'DoctorAnalysis7', 'DoctorAnalysis8', 'DoctorAnalysis9', 'DoctorAnalysis10', 'DateCreated', 'DateDue', 'InvoiceDate', 'DateShipped', 'CategoryDescription', 'StandardDescription', 'ProductId', 'ProductName', 'ProductAnalysis1', 'ProductAnalysis2', 'ProductAnalysis3', 'ProductAnalysis4', 'ProductAnalysis5', 'ProductAnalysis6', 'ProductAnalysis7', 'ProductAnalysis8', 'ProductAnalysis9', 'ProductAnalysis10', 'MaterialId', 'MaterialName', 'MaterialAnalysis1', 'MaterialAnalysis2', 'MaterialAnalysis3', 'MaterialAnalysis4', 'MaterialAnalysis5', 'MaterialAnalysis6', 'MaterialAnalysis7', 'MaterialAnalysis8', 'MaterialAnalysis9', 'MaterialAnalysis10', 'Net', 'Tax', 'Gross', 'Units', 'Qty', 'Status', 'CurrencySymbol', 'CurrencyDescription', 'Unnamed: 58', 'Unnamed: 59'],
        "Schema_5": ['Year', 'Month', 'ShipID', 'ShipFullName', 'ShipAddress', 'CustID', 'CustFullName', 'code', 'Description', 'PriceBand', 'NetUnitPrice', 'DiscountedUnitPrice', 'Qty', 'Net_Sales', 'Tax_Sales'],
        "Schema_6": ['custID', 'CustFullName', 'shipid', 'shipfullname', 'code', 'Description', 'Qty', 'Net_Sales', 'Tax_Sales'],
        "Schema_7": ['Year', 'Month', 'Practice Post Code or Identifier', 'Practice', 'Invoice.AccountReference', 'Invoice.AccountName', 'InvoiceItem.ProductAccountReference', 'Product', 'NHS /Private/Independent/PPE', 'InvoiceItem.Quantity', 'InvoiceItem.AmountNet', 'InvoiceItem.AmountVAT'],
        "Schema_8": ['Year', 'Month', 'Practice Post Code or Identifier', 'Practice Name', 'Customer Account', 'Dentist', 'Product Code', 'Product Description', 'NHS /Private/Independent/PPE', 'Quantity', 'Invoice Amount', 'Invoice VAT'],
        "Schema_9": ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11'],
        "Schema_10": ['Year', 'Month', 'shipid', 'shipfullname', 'custID', 'CustFullName', 'code', 'Description', 'Qty', 'Net_Sales', 'Tax_Sales'],
        "Schema_11":"data_sales/densign",
        "Schema_12":"data_sales/ashford"
    }
    return dic

def get_input_folder_list() -> list:
    return ["data_sales/densign","data_sales/ashford"]