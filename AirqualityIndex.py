# Import python packages
import streamlit as st
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

# Page Title
st.title("AirQuality Trend- By State/City/Day Level")
st.write("Use the dropdowns below to infer the air quality index data. Certain districts may not have pollutants data on certain dates.")

# Snowflake connection parameters
connection_parameters = {
    "ACCOUNT": "zyb38897",
    "region": "us-west-2",
    "USER": "parubits",
    "PASSWORD": "Kiki#2018",
    "ROLE": "SYSADMIN",
    "DATABASE": "dev_db",
    "SCHEMA": "consumption_sch",
    "WAREHOUSE": "reporting_wh"
}

# Create Snowflake session
session = Session.builder.configs(connection_parameters).create()

# Check if session is created
if session:
    st.write("Session created successfully!")
else:
    st.error("Failed to create a Snowflake session.")

# Variables to hold the selection parameters
state_option, city_option, date_option = '', '', ''

# Query to get distinct states from agg_city_fact_hour_level table
state_query = """
    select state from DEV_DB.CONSUMPTION_SCH.AVG_CITY_FACT_HOUR_LEVEL 
    group by state 
    order by 1 desc
"""

# Execute query using sql api and collect results
state_list = session.sql(state_query).collect()

# Extract states into a list for selectbox
state_options = [state[0] for state in state_list]  # Convert to list of states

# Use the selectbox api to render the states
state_option = st.selectbox('Select State', state_options)

# Check the selection
if state_option:
    # Query to get distinct cities from agg_city_fact_hour_level table
    city_query = f"""
    select city from DEV_DB.CONSUMPTION_SCH.AVG_CITY_FACT_HOUR_LEVEL 
    where state = '{state_option}' 
    group by city
    order by 1 desc
    """
    
    # Execute query using sql api and collect results
    city_list = session.sql(city_query).collect()

    # Extract cities into a list for selectbox
    city_options = [city[0] for city in city_list]  # Convert to list of cities

    # Use the selectbox api to render the cities
    city_option = st.selectbox('Select City', city_options)

if city_option:
    # Use predefined dates list instead of querying the Snowflake database
    predefined_dates = [
        "01-03-2024", "02-03-2024", "03-03-2024", 
        "04-03-2024", "05-03-2024"
    ]
    
    # Use the selectbox API to render the dates from the predefined list
    date_option = st.selectbox('Select Date', predefined_dates)

if date_option:
    # Convert date_option from dd-mm-yyyy to yyyy-mm-dd format
    from datetime import datetime
    
    # Convert the selected date to the correct format
    date_obj = datetime.strptime(date_option, "%d-%m-%Y")
    formatted_date = date_obj.strftime("%Y-%m-%d")  # Format it to yyyy-mm-dd

    # Query to get hourly trend data for the selected date
    trend_sql = f"""
    select 
        hour(measurement_time) as Hour,
        PM25_AVG,
        PM10_AVG,
        SO2_AVG,
        NO2_AVG,
        NH3_AVG,
        CO_AVG,
        O3_AVG
    from dev_db.consumption_sch.avg_city_fact_hour_level
    where state = '{state_option}' and city = '{city_option}' and date(measurement_time) = '{formatted_date}'
    order by measurement_time
    """
    
    # Execute query using sql api and collect results
    sf_df = session.sql(trend_sql).collect()

    # Create pandas dataframe from the Snowflake results
    pd_df = pd.DataFrame(
        sf_df,
        columns=['Hour', 'PM2.5', 'PM10', 'SO2', 'NO2', 'NH3', 'CO', 'O3']
    )

    # Display the bar chart
    st.bar_chart(pd_df.set_index('Hour'))
    st.divider()  
    st.line_chart(pd_df.set_index('Hour'))
