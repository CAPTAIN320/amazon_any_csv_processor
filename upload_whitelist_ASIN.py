# Retains ASINs if Merchant ID is in the whitelist
import glob
import pandas as pd
from functools import reduce

filename_wildcard = "csv_from_zon\*.csv"
filename_part = filename_wildcard.split('*')
csv_from_zon = glob.glob(filename_wildcard)

#print(csv_from_zon)

df_merchant_whitelist = pd.read_csv("whitelist_csv\\MerchantID_whitelist.csv")
#print(df_merchant_whitelist)

for file in csv_from_zon:
    
    file_name = file[len(filename_part[0]):-len(filename_part[1])]
    
    print("Removing ASINs from whitelisted Merchants in : "+ file_name)
    
    df = pd.read_csv(file)
    #retains only ASINs with a MerchantID
    df = df[df["MerchantID"].notnull()]
    
    #create column to hold blaclisted boolean status
    df["whitelisted"] = df["MerchantID"].isin(df_merchant_whitelist["MerchantID"])
    
    #retain rows that are not in the whitelist
    df_whitelist = df[df["whitelisted"] == True]
    
    print(df_whitelist)
    df_whitelist["ASIN"].to_csv("white_list_ASINs\\"+file_name+"_whitelist.csv",
                                index=False)
    
