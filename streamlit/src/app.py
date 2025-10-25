import streamlit as st
from utils.common import load_data
st.set_page_config(layout="wide")

# Load data
df_delay = load_data()

pages = [
    st.Page("pages/ads.py", title="Nos estimations de biens", icon="📊"),
    st.Page("pages/estimate.py", title="Estimer mon bien", icon="💵")
]

pg = st.navigation(pages)
pg.run()