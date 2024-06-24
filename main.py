import pandas as pd
import logging
from datetime import datetime

current_date = datetime.now().strftime("%Y-%m-%d")
current_time = datetime.now().strftime("%H-%M-%S")

logging.basicConfig(filename=f'logs/log_file_{current_date}_{current_time}', level=logging.INFO)


logging.info("Starting...")
logging.info("Reading the Incoming Payments")
# Read the Incoming Payments
odeme_df = pd.read_excel("gelen_odemeler.xlsx", skiprows=10)

odeme_df = odeme_df[odeme_df["DÖVİZ CİNSİ"] == "USD"] # Filter the rows with "USD" currency
odeme_df = odeme_df[["GB NO", "GB TARİH", "GB TUTAR"]] # Select only the columns that we need
odeme_df.sort_values(by="GB TUTAR", ascending=True, inplace=True) # Sort the rows by "GB TUTAR" ascending
odeme_df = odeme_df.reset_index(drop=True) # Reset the index of the dataframe
total_odeme_tutar = odeme_df["GB TUTAR"].sum() # Calculate the total amount of the Incoming Payments
logging.info(f"Incoming Payments are read. There are {len(odeme_df.index)} rows in the dataframe. Total amount of incoming Payments is {total_odeme_tutar} USD.")
logging.info(f"First 5 rows of the Incoming Payments are: {odeme_df.head(5)}")

# Read Outgoing Payments
bedel_df = pd.read_excel("gelen_bedeller.xlsx", header=None)
bedel_df.rename(columns={0 :'Company', 1: 'GB NO', 2: 'GB TUTAR'}, inplace=True )
bedel_df.sort_values(by="GB TUTAR", ascending=True, inplace=True)
bedel_df = bedel_df.reset_index(drop=True)
total_bedel_tutar = bedel_df["GB TUTAR"].sum()
logging.info(f"Outgoing Payments are read. There are {len(bedel_df.index)} rows in the dataframe. Total amount of outgoing Payments is {total_bedel_tutar} USD.")
logging.info(f"First 5 rows of the Incoming Payments are: {bedel_df.head(5)}")


logging.info("Creating the final dataframe..")
final_df = pd.DataFrame(columns=["dos_ref", "GM_kod", "GB_no", "GB_date", "GB_tutar"])


logging.info("Starting the sorting..")
odeme_tutar = 0
for index, b_row in bedel_df.iterrows():  #725
    odenecek_tutar = b_row['GB TUTAR']

    while True:
        if len(odeme_df.index) > 0:
            print(len(odeme_df.index))

            
            o_row = odeme_df.iloc[0]
            odeme_tutar = o_row['GB TUTAR']
            concat_data = []
            
            if odenecek_tutar - odeme_tutar > 0:
                odenecek_tutar -= odeme_tutar
                splitted_str = o_row['GB NO'].split("EX")

                cons_row = [b_row['GB NO'], splitted_str[0], splitted_str[1], datetime.strftime(o_row['GB TARİH'], "%-m/%-d/%y"), odeme_tutar]
                final_df.loc[len(final_df.index)] = cons_row
                odeme_df.drop(0, inplace=True)
                odeme_df.reset_index(drop=True, inplace=True)
                
                continue
            else:
                kalan_tutar = odeme_tutar - odenecek_tutar
 
                concat_data.insert(0, {'GB NO': o_row['GB NO'], 'GB TARİH': o_row['GB TARİH'], 'GB TUTAR': kalan_tutar})
                odeme_df = pd.concat([pd.DataFrame(concat_data), odeme_df], ignore_index=True) 
                

                splitted_str = o_row['GB NO'].split("EX")
                cons_row = [b_row['GB NO'], splitted_str[0], splitted_str[1], datetime.strftime(o_row['GB TARİH'], "%-m/%-d/%y"), odenecek_tutar]
                final_df.loc[len(final_df.index)] = cons_row

                odeme_df.drop(1, inplace=True)
                odeme_df.reset_index(drop=True, inplace=True)
                break
        else:
            print('Alinan ödemeler bitti.')
            break


output_file_name = "output1.xlsx"
logging.info(f"Writing dataframe into {output_file_name}")
final_df.to_excel(output_file_name)

total_final_sum = final_df["GB_tutar"].sum()
logging.info(f"Process is completed.Total amount is {total_final_sum}")