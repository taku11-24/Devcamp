from fastapi import FastAPI
from csv_DB import csv_data_format

app = FastAPI()


@app.get("/map_info")
def map_info():
    csv_data = csv_data_format()
    return csv_data


if __name__ == "__main__":
    app.run(debug=True)