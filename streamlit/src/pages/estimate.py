import streamlit as st
import pandas as pd
import numpy as np
import requests
from utils.common import write_db

st.title("Est'Immo 🏠")

st.markdown("# 💵 Estimate a property")

bedrooms_option = np.arange(1, 5, 1, dtype=int)

bathrooms_option = np.arange(1, 5, 1, dtype=int)

floors_option = np.arange(1, 5, 1, dtype=int)

year_option = np.arange(1920, 2020, 1, dtype=int)


with st.form("rental_price_predict"):

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        squared_feet = st.number_input("Property area (sq ft)", value=50.00)

        garage_size = st.number_input("Garage area (m2)", value=5.00)

        has_pool = st.toggle("Pool")


    with col2:
        num_bedrooms = st.selectbox("Number of bedrooms", bedrooms_option)

        distance_center = st.slider("Distance from the center (km)", min_value=0, max_value=50, value=10, step=1)

        has_garden = st.toggle("Garden")
        
        
    with col3:
        num_bathrooms = st.selectbox("Number of bathrooms", bathrooms_option)

        year_built = st.selectbox("Year built", year_option)

    with col4:
        num_floors = st.selectbox("Number of floors", floors_option)

        location_score = st.slider("Quality of the neighborhood", min_value=0, max_value=10, value=5, step=1)
        

    submitted = st.form_submit_button("Estimate")

    if submitted:

        payload = {
            "square_feet": squared_feet,
            "num_bedrooms": num_bedrooms,
            "num_bathrooms": num_bathrooms,
            "num_floors": num_floors,
            "year_built": year_built,
            "has_garden": int(has_garden),
            "has_pool": int(has_pool),
            "garage_size": garage_size,
            "location_score": location_score,
            "distance_to_center": distance_center
        }
        
        request = requests.post("https://qxzjy-fastapi-housing-prices.hf.space/predict", json=payload)
        response = request.json()
        
        record = pd.DataFrame.from_dict([payload])
        record["price"] = response['prediction']

        write_db(record)
        
        st.write(f"Property valuation : {round(response['prediction'], 2)} $")

        

        

        