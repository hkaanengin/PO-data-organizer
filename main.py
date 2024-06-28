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

odeme_df = odeme_df[odeme_df["DÖVİZ CİNSİ"] == "USD"]
odeme_df = odeme_df[["GB NO", "GB TARİH", "GB TUTAR"]]
odeme_df.sort_values(by="GB TUTAR", ascending=True, inplace=True)
odeme_df = odeme_df.reset_index(drop=True)
total_income_amount = odeme_df["GB TUTAR"].sum()

logging.info(f"Incoming Payments are read. There are {len(odeme_df.index)} rows in the dataframe. Total amount of incoming Payments is {total_income_amount} USD.")
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


logging.info("""
            Starting the sorting..
            \n
            --------------------------------""")

for index, b_row in bedel_df.iterrows():
    amount_to_pay = b_row['GB TUTAR']
    
    logging.info(f"Processing Outgoing Payment {b_row['GB NO']} with amount {amount_to_pay} USD.")
    while True:
        if len(odeme_df.index) > 0:
            print(len(odeme_df.index))

            
            o_row = odeme_df.iloc[0]
            income_amount = o_row['GB TUTAR']
            concat_data = []
            
            if amount_to_pay - income_amount > 0:
                #amount_to_pay -= income_amount
                splitted_str = o_row['GB NO'].split("EX")

                cons_row = [b_row['GB NO'], splitted_str[0], splitted_str[1], datetime.strftime(o_row['GB TARİH'], "%-m/%-d/%y"), income_amount]
                final_df.loc[len(final_df.index)] = cons_row
                odeme_df.drop(0, inplace=True)
                odeme_df.reset_index(drop=True, inplace=True)

                logging.info(f"{income_amount}({o_row['GB NO']}) of {amount_to_pay}({b_row['GB NO']}) has been paid. Remaining amount is {amount_to_pay-income_amount} USD.")
                amount_to_pay -= income_amount
                continue
            else:
                income_remained = income_amount - amount_to_pay
 
                concat_data.insert(0, {'GB NO': o_row['GB NO'], 'GB TARİH': o_row['GB TARİH'], 'GB TUTAR': income_remained})
                odeme_df = pd.concat([pd.DataFrame(concat_data), odeme_df], ignore_index=True) 
                

                splitted_str = o_row['GB NO'].split("EX")
                cons_row = [b_row['GB NO'], splitted_str[0], splitted_str[1], datetime.strftime(o_row['GB TARİH'], "%-m/%-d/%y"), amount_to_pay]
                final_df.loc[len(final_df.index)] = cons_row

                odeme_df.drop(1, inplace=True)
                odeme_df.reset_index(drop=True, inplace=True)
                logging.info(f"Remaining Outgoing Payment {amount_to_pay}({b_row['GB NO']}) has been paid with {income_amount}({o_row['GB NO']}).")
                break
        else:
            logging.info("There is no more Incoming Payments left to pay the Outgoing Payments.")
            break

    logging.info("--------------------------------")

output_file_name = "output1.xlsx"
final_df.to_excel(output_file_name)

total_final_sum = final_df["GB_tutar"].sum()
logging.info(f"Process is completed.Total amount is {total_final_sum}")