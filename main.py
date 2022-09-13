import streamlit as st 
import snowflake
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import col, lit,call_builtin
import json
import altair as alt
import pandas
import pydeck as pdk
import datetime
from PIL import Image
import plotly.express as px




st.set_page_config(
   page_title="Build using Snowpark and Streamlit",
   page_icon="❄️",
   layout="wide",
   initial_sidebar_state="expanded",
    menu_items={
         'Get Help': 'https://docs.snowflake.com/en/developer-guide/snowpark/index.html',
         'About': "Snowflake"
     })
     

# json_file_credentials = "/Volumes/GoogleDrive-105806873649193112765/My Drive/Demos/credentials.json"
# with open(json_file_credentials, 'r') as j:
#      credentials = json.loads(j.read())
        
connection_parameters = {
  "account": 'ch14354.eu-central-1',
  "user": 'TKO',
  "password":"TKOLondon9",
  "role": "ACCOUNTADMIN",
  "warehouse": "PYTHON",
  "database": "UK Crime Data",
 "schema": 'data'}
session = Session.builder.configs(connection_parameters).create()

# image = Image.open('/Volumes/GoogleDrive-105806873649193112765/My Drive/3.png')
# st.sidebar.image(image, caption='Made with Snowpark and Streamlit')

postcode=st.sidebar.text_input('Post Code', 'RG41 3UR')
#all_postcodes_df=session.table('"UK Crime Data".DATA.POSTCODES').select(col("Postcode 3")).collect()
#p_all_postcodes_df=pandas.DataFrame(all_postcodes_df)
#postcode = st.selectbox('What is your postcode',p_all_postcodes_df)
distance=st.sidebar.slider('Distance', 1, 20)

explain='''
Data from data.police.uk
the site for open data about crime and policing in UK.

You can download street-level crime, outcome, and stop and search data CSV format, one file per police force per month. 

This app has 8 years of crime data, we ingested over 13824 files to Snowflake using COPY and PIPE. 
'''
st.sidebar.caption(explain)

#style = st.sidebar.radio("Change map",('open-street-map', 'white-bg', 'carto-positron', 'carto-darkmatter', 'stamen- terrain', 'stamen-toner', 'stamen-watercolor'),horizontal=True)

if st.sidebar.button('run') :
    
    postcode_df=session.table('"UK Crime Data".DATA.POSTCODES').filter(col('Postcode 3')==postcode.upper()).select("Postcode 3","Local Authority Name",'''"Longitude"''','''"Latitude"''').collect()
    Latitude=postcode_df[0][3]
    Longitude=postcode_df[0][2]
    
    st.subheader('Reported crimes within '+str(distance)+' miles radius from postcode '+postcode+' ('+postcode_df[0][1]+')')

    

    df=session.table('"UK Crime Data"."DATA"."STREET_CRIME"').select((call_builtin('to_date',call_builtin('concat',col('MONTH_YY'),'-01'),'YYYY-MM-DD')).alias("month"), 'REPORTED_BY', 'FALLS_WITHIN', 'LONGITUDE', 'LATITUDE', 'LOCATION', 'LOSA_CODE', 'LOSA_NAME', 'CRIME_TYPE', 'LAST_OUTCOME_CATEGORY','CRIME_LOCATION_COORDINATE',((call_builtin('ST_DISTANCE', col('CRIME_LOCATION_COORDINATE'), call_builtin('ST_MAKEPOINT', Longitude,Latitude)) / 1609).alias("distance(miles)") )
     ).filter(
        call_builtin('round',
        call_builtin(
        'ST_DISTANCE', 
        col('CRIME_LOCATION_COORDINATE'), 
        call_builtin('ST_MAKEPOINT', Longitude,Latitude)
        ),2)
        / 1609 <= distance)
    
    df_pandas=pandas.DataFrame(df.collect())
    df_pandas['year']=pandas.DatetimeIndex(df_pandas['MONTH']).year
    
    df_search=session.table('"UK Crime Data"."DATA"."STOP_AND_SEARCH"').select((call_builtin('to_timestamp',col('DATE'),'auto')).alias("date_time"),'TYPE', 'PART_POLICING_OPS', 'LATITUDE', 'LONGITUDE', 'GENDER', 'AGE_RANGE', 'OFFICER_DEFINED_ETHNICITY', 'LEGISLATION', 'OBJECT_OF_SEARCH', 'OUTCOME', 'COORDINATES',((call_builtin('ST_DISTANCE', col('COORDINATES'), call_builtin('ST_MAKEPOINT', Longitude,Latitude)) / 1609).alias("distance(miles)") )
     ).filter(
        call_builtin('round',
        call_builtin(
        'ST_DISTANCE', 
        col('COORDINATES'), 
        call_builtin('ST_MAKEPOINT', Longitude,Latitude)
        ),2)
        / 1609 <= distance)
    
    #df_search.show(5)
    df_search_pandas=pandas.DataFrame(df_search.collect())
    
    
    #charts
    crimes_by_month_by_type=pandas.pivot_table(df_pandas,index='MONTH', columns='CRIME_TYPE', values='REPORTED_BY', aggfunc='count',fill_value=0)
     #lower case
    crimes_by_month_by_type.columns = [x.lower() for x in crimes_by_month_by_type.columns]
    
    
    df_pandas['LATITUDE'] = df_pandas['LATITUDE'].astype(float)
    df_pandas['LONGITUDE'] = df_pandas['LONGITUDE'].astype(float)
    df_pandas.columns = [x.lower() for x in df_pandas.columns]
    df_pandas['year']=pandas.DatetimeIndex(df_pandas['month']).year
    #color codings
    maps_df=df_pandas.groupby(["crime_type","latitude","longitude"]).size().reset_index()
    maps_df.columns = ["crime_type","latitude","longitude","crimes"]
    #maps_df.loc[maps_df['crime_type'] == 'Burglary', 'color'] = 1
    #maps_df.loc[maps_df['crime_type'].isnull(), 'color'] = 14


    crimes_type_df=df_pandas.groupby("crime_type").agg({'month': ['count']}).reset_index()
    crimes_type_df.columns = ['crime type', 'crimes']
    crimes_type_df['percentage']=(round((crimes_type_df['crimes']/sum(crimes_type_df['crimes'])) *100,2)).astype(str)+'%'


    crimes_outcomes_df=df_pandas.groupby("last_outcome_category").agg({'month': ['count']}).reset_index()
    crimes_outcomes_df.columns = ['outcome', 'crimes']
    crimes_outcomes_df['percentage']=(round((crimes_outcomes_df['crimes']/sum(crimes_outcomes_df['crimes'])) *100,2)).astype(str)+'%'
    

   
    base = alt.Chart(crimes_type_df.sort_values(by='crimes', ascending=False).head(5)).encode(
        theta=alt.Theta("crimes:Q", stack=True), color=alt.Color("crime type:N" )
    )

    pie = base.mark_arc()
    text = base.mark_text(radius=135, size=14).encode(text="percentage")
    
    base1 = alt.Chart(crimes_outcomes_df.sort_values(by='crimes', ascending=False).head(5)).encode(
        theta=alt.Theta("crimes:Q", stack=True), color=alt.Color("outcome:N" )
    )

    pie1 = base1.mark_arc()
    text1 = base1.mark_text(radius=135, size=14).encode(text="percentage")
    
    
    st.bar_chart(crimes_by_month_by_type)

    a, b , c,d,f,g = st.columns(6,gap='large')
    with a:
        st.metric(label="No. of Years", value=len(df_pandas['year'].unique()))
    with b:
        st.metric(label="Total Reported Crimes", value=df.count())
    with c:
        st.text('Most common outcomes')
        st.subheader(crimes_outcomes_df.sort_values(by='crimes', ascending=False).head(1)['outcome'].tolist()[0] )
    with d:
        st.text('Most common crime type')
        st.subheader(crimes_type_df.sort_values(by='crimes', ascending=False).head(1)['crime type'].tolist()[0] )
    with f:
        st.metric(label="Stop & Search", value=df_search.count())
        
    with g:
        st.text('Top Stop & Search')
        #st.metric(label="Top Stop & Search", value=df_search_pandas.groupby("OBJECT_OF_SEARCH").agg(totals=('DATE_TIME', 'count')).reset_index().sort_values('totals', ascending=False)['OBJECT_OF_SEARCH'].head(1).to_string(index=False))
        st.subheader(df_search_pandas.groupby("OBJECT_OF_SEARCH").agg(totals=('DATE_TIME', 'count')).reset_index().sort_values('totals', ascending=False)['OBJECT_OF_SEARCH'].head(1).to_string(index=False))


 
    col1, col2 = st.columns(2,gap='large')

    with col1:
        st.write(pie + text)
        st.dataframe(crimes_type_df.sort_values(by='crimes', ascending=False).style.highlight_max(axis=0))
    with col2:
        #st.bar_chart(crimes_by_month_by_type)
        st.write(pie1 + text1)    
        st.dataframe(crimes_outcomes_df.sort_values(by='crimes', ascending=False).style.highlight_max(axis=0) )
    

        
        
    
    map1, map2, map3 ,map4= st.tabs(["scatter_mapbox","Heatmap", "Scatter plot map", "Satellite map",])
    with map1:

        #'open-street-map', 'white-bg', 'carto-positron', 'carto-darkmatter', 'stamen- terrain', 'stamen-toner', 'stamen-watercolor'
        fig = px.scatter_mapbox(maps_df, lat="latitude", lon="longitude",
                                color="crime_type", size="crimes", center=dict(lat=Latitude, lon=Longitude),
                                mapbox_style="stamen-toner",
                                #mapbox_style=style,
                                zoom=13, 
                                height=600,
                                title=''
                               )

        #fig1 = px.scatter_geo(pandas.DataFrame(postcode_df),lat="Latitude", lon="Longitude")
        #fig.add_traces(fig1._data)
        #fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        #st.write(fig.show())
        st.plotly_chart(fig, use_container_width=True)




    
    
    with map2:
        st.pydeck_chart(pdk.Deck(
             map_style='mapbox://styles/mapbox/outdoors-v11',
             initial_view_state=pdk.ViewState(
                 latitude=Latitude,
                 longitude=Longitude,
                 zoom=11,
                 pitch=10,
                 width=900
             ),
             layers=[


                pdk.Layer(
                    "HeatmapLayer",
                    data=maps_df[['latitude','longitude']],
                    opacity=0.9,
                    get_position=["longitude", "latitude"],
                    aggregation='COUNT',
                    get_weight=2) ,

                pdk.Layer(
                    'ScatterplotLayer',
                    data=[Longitude,Latitude],
                    get_position=[Longitude,Latitude],
                    get_color=[255, 30, 0, 50],
                    get_radius=100,
                    radius_scale=1),
             ],
         ))
    with map3:
        st.pydeck_chart(pdk.Deck(
             map_style='mapbox://styles/mapbox/streets-v11',
             initial_view_state=pdk.ViewState(
                 latitude=Latitude,
                 longitude=Longitude,
                 zoom=11,
                 pitch=10,
                 width=900
             ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=df_pandas[['latitude','longitude']],
                get_position='[longitude, latitude]',
                get_color=[200, 30, 0, 160],
                get_radius=1214,
                radius_scale=0.05
             ),
            pdk.Layer(
                "HexagonLayer",
                data=[Latitude,Longitude],
                get_position=[Longitude,Latitude],
                radius=70,
                extruded=True,
                get_color=[200, 30, 0, 0.5],
                elevation_scale=4
            )

         ],
         ))
    with map4:
        st.pydeck_chart(pdk.Deck(
             map_style='mapbox://styles/mapbox/satellite-streets-v11',
             initial_view_state=pdk.ViewState(
                 latitude=Latitude,
                 longitude=Longitude,
                 zoom=11,
                 pitch=10,
                 width=900
             ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=df_pandas[['latitude','longitude']],
                get_position='[longitude, latitude]',
                get_color=[200, 30, 0, 160],
                get_radius=700,
                radius_scale=0.05
             ),
            pdk.Layer(
                "HexagonLayer",
                data=[Latitude,Longitude],
                get_position=[Longitude,Latitude],
                radius=90,
                extruded=True,
                get_color=[200, 30, 0, 0.5],
                elevation_scale=4
            )

         ],
         ))
        
    
    tab1, tab2 = st.tabs(["Python", "Sql"])
    
    with tab1:
        
    
        code = '''
import streamlit 
from snowflake.snowpark import Session
import pandas

postcode=streamlit.text_input('Enter Post Code')
postcode_df=session.table('DATA.POSTCODES').filter(col('Postcode')==postcode).collect()

snowpark_df=session.table('STREET_CRIME')
 .filter(call_builtin
        (  'ST_DISTANCE', 
            col('CRIME_LOCATION_COORDINATE'), 
            call_builtin('ST_MAKEPOINT', Longitude,Latitude)
        )
    /1609 <= distance)

streamlit.metric(label="Total Reported Crimes", value=df.count())
streamlit.map(maps_df[['latitude','longitude']] )

    '''
        st.code(code, language='python')
    with tab2:
        
        code1 = '''
SELECT
    "MONTH",
    "REPORTED_BY",
    "FALLS_WITHIN",
    "LONGITUDE",
    "LATITUDE",
    "LOCATION",
    "LOSA_CODE",
    "LOSA_NAME",
    "CRIME_TYPE",
    "LAST_OUTCOME_CATEGORY",
    "CRIME_LOCATION_COORDINATE"
    (
        ST_DISTANCE(
            "CRIME_LOCATION_COORDINATE",
            ST_MAKEPOINT(-0.770821, 51.399417)
        ) / 1609 :: bigint
    ) AS "distance(miles)"
FROM
"UK Crime Data"."DATA"."STREET_CRIME"
)
WHERE
    (
        (
                ST_DISTANCE(
                    "CRIME_LOCATION_COORDINATE",
                    ST_MAKEPOINT(-0.770821, 51.399417)
                ),
                2
             / 1609 :: bigint
        ) <= 1 :: bigint
    '''
        st.code(code1, language='sql')

    
    session.close()

