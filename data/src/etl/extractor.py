import pandas as pd
import json


def read_json_to_df(file_path):
    with open(file_path, "r") as file:
        json_data = json.load(file)

        df = pd.DataFrame(json_data["transactions"])

        print("Number of records: ", len(df))
        df.to_csv("json_output.csv", index=False)

        return df
