import glob
import pandas as pd

def csv_merchant_octo():
    merchant_id_octo = []
    for url in df_merchant_octo["Page_URL"]:
        merchant_id_from_url = url[135:-10]
        merchant_id_octo.append(merchant_id_from_url)

    df_merchant_octo["merchant_id_octo"] = merchant_id_octo
    print(df_merchant_octo["merchant_id_octo"])

    df_merchant_octo["business_address"] = df_merchant_octo["business_address"].astype(str)
    country_merchant_octo = []
    for merchant_address in df_merchant_octo["business_address"]:
        country_merchant = merchant_address[-2:]
        country_merchant_octo.append(country_merchant)

    df_merchant_octo["country_merchant_octo"] = country_merchant_octo

    merge_df = df_zon_processed.merge(df_merchant_octo, 
                                    left_on="MerchantID", 
                                    right_on="merchant_id_octo",
                                    how="left")
    
    return merge_df


def csv_product_octo():
    df_product_octo["seller_address"] = df_product_octo["seller_address"].astype(str)
    country_product_octo = []
    for product_address in df_product_octo["seller_address"]:
        country_product = product_address[-2:]
        country_product_octo.append(country_product)

    df_product_octo["country_product_octo"] = country_product_octo


    merge_df = csv_merchant_octo().merge(df_product_octo,
                                    left_on="ASIN",
                                    right_on="ASIN_from_page_url",
                                    how="left")

    print(merge_df.count())
    return merge_df


def merge_csv(merge_df):# (ouptut_of_merchant, output_of_product)
    print()


def export_concantenated_csv(merge_df):
    merge_df = merge_df[["ASIN", 
                         "Brand",
                         "SoldBy", 
                         "MerchantID", 
                         "country_merchant_octo",
                         "country_product_octo"]]
    merge_df.to_csv("concantenated\\" + file_name + "_concantenated.csv")

def export_ASIN_csv(merge_df):
    merge_df = merge_df[["ASIN"]]
    merge_df.to_csv("concantenated_ASIN_ONLY\\" + file_name + "_concantenated_asin_only.csv",
                    index=False)

def export_html(merge_df):
    merge_df = merge_df[["ASIN",
                         "Brand",
                         "SoldBy",
                         "MerchantID",
                         "country_merchant_octo",
                         "country_product_octo"]]
    merge_df.to_html("html\\" + file_name + ".html", escape=False)




csv_merchant_octo_folder = glob.glob("csv_merchant_octo\*_merchant_octo.csv")

for file in csv_merchant_octo_folder:

  file_name = file[18:-18]
  
  print("Processing & Merging: "+ file_name.title())
  
  df_zon_processed = pd.read_csv("csv_from_zon_processed\\" + file_name + "_processed.csv")
  #print(df_zon_processed.count())
  
  df_merchant_octo = pd.read_csv("csv_merchant_octo\\" + file_name + "_merchant_octo.csv")
  #print(df_merchant_octo.count())
  
  df_product_octo = pd.read_csv("csv_product_octo\\" + file_name + "_product_octo.csv")
  #print(df_product_octo.count())
  
  csv_merchant_octo()
  merge_df = csv_product_octo()
  export_concantenated_csv(merge_df)
  export_ASIN_csv(merge_df)
  export_html(merge_df)

