import requests
from datetime import datetime, timedelta
# import json
import logging

## Comment this if you want to run this single file
from config import ENV
from global_utils.send_telegram_message import send_telegram_message
## Uncomment this if you want to run this single file
# from ..config import ENV
# from ..global_utils.send_telegram_message import send_telegram_message


def get_ngi_data():
    try:
        ngi_email = ENV.NGI_EMAIL
        ngi_password = ENV.NGI_PASSWORD.get_secret_value()
        ngi_endpoint = ENV.NGI_ENDPOINT
        bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
        chat_id = ENV.TELEGRAM_GROUP_CHAT_ID

        auth_endpoint = f"{ngi_endpoint}auth"
        issue_date = datetime.now().strftime("%Y-%m-%d")
        datafeed_endpoint = f"{ngi_endpoint}forwardDatafeed.json?issue_date={issue_date}" 

        # """ Step 1: Retrieve JWT """
        data = {
            "email":  ngi_email, # subscriber email
            "password": ngi_password
        }

        response = requests.post(auth_endpoint, json=data)
        if not response.ok:
            logging.error(f"Failed to authenticate: {response.status_code} - {response.text}")
            send_telegram_message(bot_token, chat_id, f"Failed to authenticate with NGI API. Status code: {response.status_code}")

        key_json = response.json()
        authToken = key_json.get("access_token")
        if not authToken:
            send_telegram_message(bot_token, chat_id, "Access token not found in NGI authentication response.")
            raise ValueError("Access token not found in response")

        # """ Step 2: Pass back JWT in header of query to retrieve data """
        head = {'Authorization': f'Bearer {authToken}'}

        responseData = requests.get(datafeed_endpoint, headers=head)
        responseData.raise_for_status()
        ngi_data = responseData.json()
        if "data" not in ngi_data or "meta" not in ngi_data:
            raise ValueError("Invalid response structure")

        responseData = requests.get(datafeed_endpoint, headers=head)
        ngi_data = responseData.json()  # Parse the response as JSON

        data = ngi_data["data"]
        meta = ngi_data["meta"]
        gas_data = []

        keys_to_retrieve = [["SLAHH", "HH"], ["ETXHSHIP", "HSC"], ["WTXEPP", "EP"],  ["CALSAVG", "SCL"], ["WTXWAHA", "WAH"]]

        for i in keys_to_retrieve:
            data_retrieved = data[i[0]]
            for j in range(len(data_retrieved["Contracts"])):
                trade_date = meta["trade_date"]
                flow_date = data_retrieved["Contracts"][j]
                fix_flow_date = datetime.strptime(flow_date, "%Y-%m-%d")
                fix_flow_date = fix_flow_date + timedelta(days=1)
                fix_flow_date = fix_flow_date.strftime("%Y-%m-%d")
                price = data_retrieved["Fixed Prices"][j]

                gas_data.append({
                    "tradeDate": trade_date,
                    "flowDate": fix_flow_date,
                    "indice": i[1],
                    "precio": price,
                    "fuente": "NGI",
                    "usuario": "becario",
                    "fechaCreacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "fechaActualizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })


        """ Step 3: Write data to the database """
        # # Write the data to a JSON file
        # output_filename = f"response-{issue_date}.json"

        # with open(output_filename, "w") as json_file:
        #     json.dump(gas_data, json_file, indent=4)

        # send_telegram_message(bot_token, chat_id, f"Data successfully processed and saved to {output_filename}")

        response = requests.post("http://127.0.0.1:8080/api/v1/gas", json=gas_data)
        # Send a Telegram message
        if response.status_code == 201:
            logging.info("Data has been written to the database")
            send_telegram_message(bot_token, chat_id, "Data successfully processed and saved to the database.")
        else:
            logging.error(f"Failed to write data: {response.status_code} - {response.text}")
            send_telegram_message(bot_token, chat_id, f"Failed to save data. Status code: {response.status_code}")

    except Exception as e: 
        logging.error(f"An unexpected error occurred: {e}")