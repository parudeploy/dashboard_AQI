# Import python packages
import streamlit as st
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

# Page Title
st.title("AirQuality Trend- By State/City/Day Level")
st.write("Use the dropdowns below to infer the air quality index data.Certain districts may not have pollutants data on certain dates.")

# Snowflake connection parameters
connection_parameters = {
    "ACCOUNT": "dx53793",
    "region": "ap-southeast-1",
    "USER": "marisnowflake",
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
    # Query to get distinct dates from the table
    date_query = f"""
        select date(measurement_time) as measurement_date 
        from DEV_DB.CONSUMPTION_SCH.AVG_CITY_FACT_HOUR_LEVEL 
        where state = '{state_option}' and city = '{city_option}'
        group by measurement_date
        order by 1 desc
    """
    
    # Execute query using sql api and collect results
    date_list = session.sql(date_query).collect()

    # Extract dates into a list for selectbox
    date_options = [str(date[0]) for date in date_list]  # Convert to list of dates

    # Use the selectbox api to render the dates
    date_option = st.selectbox('Select Date', date_options)

if date_option:
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
    where state = '{state_option}' and city = '{city_option}' and date(measurement_time) = '{date_option}'
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
