import pandas as pd
import mlflow
from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile, HTTPException

description = """
This API provide endpoints for housing prices predictions

* `/predict` To predict the price of a house based on its characteristics, on one observation
* `/batch-predict` To predict the price of a house based on its characteristics, on a batch of observation
"""

tags_metadata = [

    {
        "name": "Machine-Learning"
    }
]

app = FastAPI(
    title="Housing prices predictions",
    description=description,
    version="0.1",
    openapi_tags=tags_metadata
)

MODEL_NAME = "housing-prices-estimator-LR"
MODEL_VERSION = "latest"

class PredictionFeatures(BaseModel):
    square_feet: float
    num_bedrooms: int
    num_bathrooms: int
    num_floors: int
    year_built: int
    has_garden: int
    has_pool: int
    garage_size: int
    location_score: float
    distance_to_center: float

@app.post("/predict", tags=["Machine-Learning"])
async def predict(predictionFeatures: PredictionFeatures):
    """
    Prediction for one observation. Endpoint will return a dictionnary like this:

    ```
    {'prediction': PREDICTION_VALUE}
    ```

    You need to give this endpoint all columns values as dictionnary, or form data.
    """
    try:
        # Read data
        df = pd.DataFrame([predictionFeatures.dict()])

        # Load model
        loaded_model= mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")

        prediction = loaded_model.predict(df)

        # Format response
        response = {"prediction": prediction.tolist()[0]}

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/batch-predict", tags=["Machine-Learning"])
async def batch_predict(file: UploadFile = File(...)):
    """
    Make prediction on a batch of observation. This endpoint accepts only **csv files** containing
    all the trained columns WITHOUT the target variable.
    """
    try:
        # Read file
        df = pd.read_csv(file.file, index_col="id")

        # Load model
        loaded_model= mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")
        
        predictions = loaded_model.predict(df)

        return predictions.tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))