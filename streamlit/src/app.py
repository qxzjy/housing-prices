import streamlit as st
from utils.common import load_data
st.set_page_config(layout="wide")

# Load data
df_delay = load_data()

# Intializing pages
pages = [
    st.Page("pages/ads.py", title="Database", icon="📊"),
    st.Page("pages/estimate.py", title="Estimate a property", icon="💵")
]

pg = st.navigation(pages)
pg.run()