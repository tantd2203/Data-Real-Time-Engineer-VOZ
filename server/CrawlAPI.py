from flask import Flask, jsonify
import pandas as pd
import random

app = Flask(__name__)

def read_excel_file(file_path):
    df = pd.read_excel(file_path)
    data = df.to_dict(orient='records')
    return data

@app.route('/', methods=['GET'])
def get_random_row():
    data = read_excel_file('../data/dataset.xlsx')
    if data:
        random_row = random.choice(data)
        return jsonify(random_row)
    else:
        return jsonify({"error": "No data found"}), 404

if __name__ == '__main__':
    app.run(debug=True)
