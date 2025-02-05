import pandas as pd
import gspread
from pymongo import MongoClient
from flask import Flask, request, jsonify
from flask_cors import CORS
import itertools
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.environ["MONGO_URI"]
client = MongoClient(MONGO_URI)

app = Flask(__name__)
CORS(app)

PRODUCT_CODE = "tp_a-frame"
KEY = "sheets_key_new.json"

service_acc = gspread.service_account(KEY)
attribute_sheet_id = "12cyM8-Azt8JhrytdE99p202bROfpb21QvONthrwHFVI"
attribute_sheet = service_acc.open_by_key(attribute_sheet_id)
attribute_codes_sheet = attribute_sheet.worksheet("Attributes")
attribute_data = attribute_codes_sheet.get()

# Creating DataFrame
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', 5000)
attributes = pd.DataFrame(attribute_data[1:], columns=attribute_data[0])

cols = ["Pages", "Paper", "Colour", "Format",  "Finishing",
        "Extra", "Binding", "Refinement", "Quantity"]
cols1 = ["Sheets", "Paper", "Colour", "Format",
         "Finishing", "Extra", "Binding", "Refinement"]
df_cols = ['Product Name', 'Product Code', 'category', 'pages', 'finishing', 'binding', 'extra', 'format', 'quantity',
           'refinement', 'colour', 'paper', 'Printing Markup', 'Finishing Markup', 'Binding Markup', 'Option Markup', 'Refinement Markup']
attributes_cols = ['pages', 'finishing', 'binding', 'extra',
                   'format', 'quantity', 'refinement', 'colour', 'paper']


def get_product_data(product_code: str) -> pd.DataFrame:
    db = client["Printulu"]
    products = db["products"]
    product_data = products.find_one({"product_code": product_code})
    printing_makrup = product_data["markup"]["Printing"]
    binding_markup = product_data["markup"]["Binding"]
    extra_markup = product_data["markup"]["Option"]
    finishing_markup = product_data["markup"]["Finishing"]
    refinement_markup = product_data["markup"]["Refinement"]
    options = [product_data[key] for key in ['category', 'pages', 'finishing', 'binding', 'extra', 'format', 'quantity', 'refinement', 'colour', 'paper']]
    combinations = list(itertools.product(*options))
    product_df = pd.DataFrame(combinations, columns=['category', 'pages', 'finishing', 'binding', 'extra', 'format', 'quantity', 'refinement', 'colour', 'paper'])
    product_df["Printing Markup"] = printing_makrup
    product_df["Binding Markup"] = binding_markup
    product_df["Option Markup"] = extra_markup
    product_df["Finishing Markup"] = finishing_markup
    product_df["Refinement Markup"] = refinement_markup
    product_df["Product Code"] = product_code
    product_df["Product Name"] = product_data["product_name"]
    return product_df



def create_combinations(product_code):
    product_df = get_product_data(product_code)
    new_cols = df_cols.copy()
    for col in product_df.columns:
        if col in attributes_cols:
            col_lookup = col.title()
            attributes_product_df = attributes[attributes["Type"] == col_lookup].drop_duplicates().reset_index(drop=True)
            merged = pd.merge(product_df, attributes_product_df, "left",right_on="code", left_on=col).reset_index(drop=True)
            col_codes = merged["attribute_name"]
            product_df[col_lookup] = col_codes
            new_cols.append(col_lookup) if col_lookup not in new_cols else None
    # TODO: Include Negative rules here
    return product_df[new_cols].to_dict(orient="records")


@app.route('/api/<product_code>', methods=['GET'])
def get_product(product_code):
    return create_combinations(product_code)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
