import glob
import pandas as pd
from functools import reduce

filename_wildcard = "concantenated/*_concantenated.csv"
filename_part = filename_wildcard.split('*')
print(filename_part)
csv_concantenated = glob.glob(filename_wildcard)

#print(csv_concantenated)

blacklist_merchant_array = []
blacklist_product_array = []
for file in csv_concantenated:

  file_name = file[len(filename_part[0]):-len(filename_part[1])]
  print(file_name)
  
  print("Extracting blacklists from: "+ file_name.title())
  
  df_concantenated = pd.read_csv(file)
  #print(df_concantenated)

  df_merchant = df_concantenated[["MerchantID", 
                                  "country_merchant_octo"]]
  
  df_product = df_concantenated[["Brand",
                                 "country_product_octo"]]
  
  #get Chinese rows from merchant_octo
  df_blacklist_merchant = df_merchant.loc[df_merchant["country_merchant_octo"] == "CN"]
  #print(df_blacklist_merchant)
  
  blacklist_merchant_array.append(df_blacklist_merchant)
  
  #get Chinese rows from product_octo
  df_blacklist_product = df_product.loc[df_product["country_product_octo"] == "CN"]
  #print(df_blacklist_product)
  
  blacklist_product_array.append(df_blacklist_product)
  


#merge blacklist of merchant IDs
df_blacklist_merchant_merged = reduce(lambda left,right: pd.merge(left,
                                                right,
                                                how='outer'),
                                       blacklist_merchant_array)
print(len(blacklist_merchant_array) , "merchant files merged to form blacklist.")

#removes duplicate MerchantID
df_blacklist_merchant_merged = df_blacklist_merchant_merged.drop_duplicates(subset=["MerchantID"],
                                                                            keep="first")

#exports merchant blacklist as csv file
df_blacklist_merchant_merged.to_csv("blacklist_csv/MerchantID_blacklist.csv",
                                    index=False)
print(df_blacklist_merchant_merged)


#merge blacklist of merchant IDs
df_blacklist_product_merged = reduce(lambda  left,right: pd.merge(left,
                                                right,
                                                how='outer'),
                                       blacklist_product_array)
print(len(blacklist_product_array) , "merchant files merged to form blacklist.")

#removes duplicate Brand
df_blacklist_product_merged = df_blacklist_product_merged.drop_duplicates(subset=["Brand"],
                                                                          keep="first")

#exports Brand blacklist as csv file
df_blacklist_product_merged.to_csv("blacklist_csv/Brand_blacklist.csv",
                                   index=False)


