import pandas as pd
import time
import mlflow
import os
from mlflow.models.signature import infer_signature
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import Lasso
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine

def load_data():
    engine = create_engine(os.getenv("DB_URI"), echo=True)

    with engine.connect() as conn :
        return pd.read_sql("SELECT * FROM housing_prices", conn, index_col="id")

def preprocess_data(dataset):
    target_variable = "price"

    X = dataset.drop([target_variable] , axis = 1)
    X = X.set_axis(["square_feet", "num_bedrooms", "num_bathrooms", "num_floors", "year_built", "has_garden", "has_pool", "garage_size", "location_score", "distance_to_center"], axis=1)
    y = dataset[target_variable]

    return train_test_split(X, y, test_size = 0.2, random_state = 3)

def create_pipeline():

    return Pipeline(steps=[
        ("standard_scaler", StandardScaler()),
        ("lasso",Lasso())
    ], verbose=True)

def train_model(pipeline, X_train, y_train, param_grid, cv=3):
    model = GridSearchCV(pipeline, param_grid, cv=cv, scoring="r2")
    model.fit(X_train, y_train)

    return model

def log_model(model, X_train, artifact_path, registered_model_name):
    predictions = model.predict(X_train)

    try :
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path=artifact_path,
            registered_model_name=registered_model_name,
            signature=infer_signature(X_train, predictions)
        )
    except mlflow.exceptions.MlflowException as e:
        print("⚠️ Model logging failed:", e)

if __name__ == "__main__":

    param_grid = {
        "lasso__alpha": [i for i in range(100, 1000, 25)]
    }
    
    print("Training model...")
    
    # Time execution
    start_time = time.time()

    # Call mlflow autolog
    mlflow.sklearn.autolog(log_models=False) # We won't log models right away

    # Load data from DB
    dataset = load_data()

    # X, y split
    X_train, X_test, y_train, y_test = preprocess_data(dataset)
    
    # Pipeline
    pipeline = create_pipeline()

    # Log experiment to MLFlow
    with mlflow.start_run() as run:
        model = train_model(pipeline, X_train, y_train, param_grid)
        log_model(model, X_train, "housing-prices-estimator", "housing-prices-estimator-LR")
        
    print("...Done!")
    print(f"---Total training time: {time.time()-start_time}")

    