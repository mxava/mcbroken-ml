import query as sql
import sequencer as seq
import json

from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI()

# TODO: define API schema
# TODO: define mcflurry data_contents schema
# TODO: define weather data_contents schema

def data_contents_isvalid(data_contents: json) -> bool:
    # TODO: write validation logic
    return True # you are SO valid, bestie

@app.post("/data/mcflurry/{data_contents}")
async def add_mcflurry_data(data_contents: json) -> json:
    if data_contents:
        if data_contents_isvalid:
        ### data validation, return error message if invalid data type
           ### add item to queue
           ### write data to file
           return {"message": "Your data has been added to the queue and will be processed shortly."}
        return {"error": "Payload contains invalid data format"}
    return {"error": "Payload must not be empty"}


@app.put("/data/mcflurry/{data_contents}")
async def update_mcflurry_dataset(data_contents: json):
    pass

@app.post("/data/weather/{data_contents}")
async def add_weather_data(data_contents: json) -> json:
    pass

@app.put("/data/weather/{data_contents}")
async def update_mcflurry_dataset(data_contents: json):
    pass