import glob
import pandas as pd
from functools import reduce

filename_wildcard = "concantenated/*_concantenated.csv"
filename_part = filename_wildcard.split('*')
csv_concantenated = glob.glob(filename_wildcard)

print(csv_concantenated)


whitelist_merchant_array = []
whitelist_product_array = []
for file in csv_concantenated:
  
  country_array = ["US", "GB", "GR", "CA", "AU", "KR", "FR"]

  for country in country_array:

    file_name = file[len(filename_part[0]):-len(filename_part[1])]
    
    print("Extracting whitelists from: "+ file_name.title())
    
    df_concantenated = pd.read_csv(file)
    #print(df_concantenated)

    df_merchant = df_concantenated[["MerchantID", 
                                    "country_merchant_octo"]]
    
    df_product = df_concantenated[["Brand",
                                  "country_product_octo"]]
    
    #get US rows from merchant_octo
    df_whitelist_merchant = df_merchant.loc[df_merchant["country_merchant_octo"] == country]
    #print(df_whitelist_merchant)
    
    whitelist_merchant_array.append(df_whitelist_merchant)
    
    #get US rows from product_octo
    df_whitelist_product = df_product.loc[df_product["country_product_octo"] == country]
    #print(df_whitelist_product)
    
    whitelist_product_array.append(df_whitelist_product)
  


#merge whitelist of merchant IDs
df_whitelist_merchant_merged = reduce(lambda  left,right: pd.merge(left,
                                                right,
                                                how='outer'),
                                       whitelist_merchant_array)
print(len(whitelist_merchant_array) , "merchant files merged to form whitelist.")

#removes duplicate MerchantID
df_whitelist_merchant_merged = df_whitelist_merchant_merged.drop_duplicates(subset=["MerchantID"],
                                                                            keep="first")

#exports merchant whitelist as csv file
df_whitelist_merchant_merged.to_csv("whitelist_csv/MerchantID_whitelist.csv",
                                    index=False)
print(df_whitelist_merchant_merged)


#merge whitelist of merchant IDs
df_whitelist_product_merged = reduce(lambda  left,right: pd.merge(left,
                                                right,
                                                how='outer'),
                                       whitelist_product_array)
print(len(whitelist_product_array) , "merchant files merged to form whitelist.")

#removes duplicate Brand
df_whitelist_product_merged = df_whitelist_product_merged.drop_duplicates(subset=["Brand"],
                                                                          keep="first")

#exports Brand whitelist as csv file
df_whitelist_product_merged.to_csv("whitelist_csv/Brand_whitelist.csv",
                                   index=False)



