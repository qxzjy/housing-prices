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
async def predict(predictionFeatures: dict[str, int | float]) -> dict[str, float]:
    """
    Prediction of the price of a house based on its characteristics,for one observation.
    
    Args:
        predictionFeatures (dict[str, int | float]): One observation of house to predict his price.
    
    Returns:
        dict[str, float]: House price predicted within the format ```{'prediction': PREDICTION_VALUE}```

    Raises:
        HTTPException: If any error occurs related to the MLflow server.
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
async def batch_predict(file: UploadFile = File(...)) -> dict:
    """
    Make prediction on a batch of observation. This endpoint accepts only **csv files** containing
    all the trained columns WITHOUT the target variable.
    
    Args:
        file (UploadFile): Batch of observation of houses to predict their price, CSV files.
    
    Returns:
        dict[str, float]: Houses price predicted within the format ```{'prediction': PREDICTION_VALUE}```

    Raises:
        HTTPException: If any error occurs related to the MLflow server.
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