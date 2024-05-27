import streamlit as st
import pandas as pd
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery


#------------------------- Configuration --------------------------------------#
st.set_page_config(
    page_title="Business Dashboard App",
    layout="wide",
    initial_sidebar_state="expanded",
    )

st.html("styles.html")
#------------------------- SQL Connection --------------------------------------#

### BigQuery connection
credentials = service_account.Credentials.from_service_account_info(
     st.secrets["gcp_service_account"]
)

client = bigquery.Client(credentials=credentials)


#------------------------ SQL Query --------------------------------------#
# BigQuery Query
@st.cache_data()
def get_aum_data():
    query = """
        SELECT 
            client_aum.*,
            client_main.company_name
        FROM `streamlit-apps-424120.business_db.client_aum` client_aum
        LEFT JOIN `streamlit-apps-424120.business_db.client_main` client_main
        ON client_aum.internal_id = client_main.internal_id
        ORDER BY client_aum.aum_date
    """
    return client.query(query).to_dataframe()

aum_data = get_aum_data()

# BigQuery Query
@st.cache_data()
def get_data():
    query = """
    WITH latest_figures AS (
        SELECT 
            internal_id, 
            ARRAY_AGG(STRUCT(aum, aum_date, contributions, active_members, deferred_members) 
                    ORDER BY aum_date DESC LIMIT 1)[OFFSET(0)].*
        FROM `streamlit-apps-424120.business_db.client_aum` client_aum
        GROUP BY internal_id
    ),
    latest_fees AS (
        SELECT
            internal_id,
            ARRAY_AGG(STRUCT(fee_date, aum_bps, additional_fee_bps) 
                    ORDER BY fee_date DESC LIMIT 1)[OFFSET(0)].*
        FROM `streamlit-apps-424120.business_db.client_fee` client_fee
        GROUP BY internal_id
    )

    SELECT 
        client_main.internal_id, 
        client_main.company_name,
        client_main.product_type,
        client_main.status,
        client_main.client_representative,
        client_main.contact_name,
        client_main.business_type,
        client_main.website,
        client_main.description,
        client_main.city,
        client_main.email,
        client_main.onboarding_date,
        client_main.created_at,
        latest_figures.aum, 
        latest_figures.aum_date, 
        latest_figures.contributions, 
        latest_figures.active_members, 
        latest_figures.deferred_members,
        latest_figures.active_members + latest_figures.deferred_members AS total_members,
        latest_fees.aum_bps,
        latest_fees.additional_fee_bps,
        latest_fees.aum_bps * latest_figures.aum AS annual_revenue
    FROM `streamlit-apps-424120.business_db.client_main` client_main
    LEFT JOIN latest_figures
    ON client_main.internal_id = latest_figures.internal_id
    LEFT JOIN latest_fees
    ON client_main.internal_id = latest_fees.internal_id
    --GROUP BY client_main.internal_id, latest_figures.aum, latest_figures.aum_date, latest_figures.contributions, latest_figures.active_members, latest_figures.deferred_members, latest_fees.aum_bps, latest_fees.additional_fee_bps
    GROUP BY ALL
    ORDER BY latest_figures.aum_date;
    """
    return client.query(query).to_dataframe()

df = get_data()

#------------------------- Data cleansing --------------------------------------#


aum_data.drop(columns=['_airbyte_raw_id',
                        '_airbyte_extracted_at',
                        '_airbyte_meta'], inplace=True)


#------------------------- Data Summary --------------------------------------#
df['onboarding_date'] = pd.to_datetime(df['onboarding_date'])
df['onboarding_year'] = df['onboarding_date'].dt.year

aum_data['aum_date'] = pd.to_datetime(aum_data['aum_date']).dt.date

tiles_upper = st.columns([1,1,1,1,1])
tiles_lower = st.columns([1,1,1,1,1])

st.divider()

total_aum_in_billion = df["aum"].sum() / 1_000_000_000

tiles_upper[0].metric('Total AUM', f'£{total_aum_in_billion:.2f}B')
tiles_upper[1].metric('Total Revenue', f'£{df["annual_revenue"].sum():,.0f}')
tiles_upper[2].metric('Total Contributions', f'£{df["contributions"].sum():,.0f}')
tiles_upper[3].metric('Total Active Members', f'{df["active_members"].sum():,.0f}')
tiles_upper[4].metric('Total Deferred Members', f'{df["deferred_members"].sum():,.0f}')
tiles_lower[0].metric('Total Members', f'{df["total_members"].sum():,.0f}')
tiles_lower[1].metric('Total Clients', df.shape[0])
tiles_lower[2].metric('Internal Clients', df[df['status'] == 'Internal'].shape[0])
tiles_lower[3].metric('Prospect Clients', df[df['status'] == 'Prospect'].shape[0])
tiles_lower[4].metric('Lost Clients', df[df['status'] == 'Lost'].shape[0])

#------------------------- Data Visualization ----------------------------------#
charts = st.columns([1,1])


fig = px.histogram(df, x='aum', title='AUM Distribution')
charts[0].plotly_chart(fig)


fig = px.histogram(df, x='product_type', title='Product Type Distribution')
charts[1].plotly_chart(fig)


highlights = st.columns([1,1,1])

max_aum_index = df['aum'].idxmax()
company_with_max_aum = df.loc[max_aum_index, 'company_name']
max_aum = df.loc[max_aum_index, 'aum']

highlights[0].write('#### Highest AUM')
with highlights[0]:
    st.metric(f'{company_with_max_aum}',f'£{max_aum:,.0f}')


min_aum_index = df['aum'].idxmin()
company_with_min_aum = df.loc[min_aum_index, 'company_name']
min_aum = df.loc[min_aum_index, 'aum']

highlights[1].write('#### Lowest AUM')
with highlights[1]:
    st.metric(f'{company_with_min_aum}',f'£{min_aum:,.0f}')


max_contributions_index = df['contributions'].idxmax()
company_with_max_contributions = df.loc[max_contributions_index, 'company_name']
max_contributions = df.loc[max_contributions_index, 'contributions']

highlights[2].write('#### Highest Contributions')
with highlights[2]:
    st.metric(f'{company_with_max_contributions}',f'£{max_contributions:,.0f}')

st.divider()

#------------------------- Charts --------------------------------------#
col1, col2, col3 = st.columns([1,1,1])

#bar chart
with col1:
    fig = px.bar(df, x='product_type', y='aum', title='AUM by Product Type')
    st.plotly_chart(fig)

#pie chart
with col2:

    fig = px.pie(df, names='product_type', title='Client Status')
    st.plotly_chart(fig)

#scatter plot
with col3:
    trace = px.scatter(aum_data, x='aum_date', y='aum', color='company_name', title='AUM Trend')
    st.plotly_chart(trace)


#------------------------- Company Card --------------------------------------#
container = st.container(border=True, height=1000)
container.subheader('Company CARD')
cols = container.columns(3)

company = cols[0].selectbox('Select Company:', aum_data['company_name'])
cols[2].container(border=True).write(f'**Reporting Date:** {aum_data["aum_date"].max().strftime("%Y-%m-%d")}')

company_aum_data = aum_data[aum_data['company_name'] == company]
company_data = df[df['company_name'] == company].iloc[0]

trace = px.line(company_aum_data, x='aum_date', y='aum', title='AUM Trend')
container.plotly_chart(trace)

cols = container.columns(4)


cols[0].markdown("""
    <div style="background-color: #005580; padding: 0.2px; border-radius: 10px; margin-bottom: 20px;">
        <h4 style="color: white; padding-left: 10px;"> Company Details</h4>
    </div>
""", unsafe_allow_html=True)

cols[0].write(f'**Company Name:** {company_data["company_name"]}')
cols[0].write(f'**Product Type:** {company_data["product_type"]}')
cols[0].write(f'**Status:** {company_data["status"]}')
cols[0].write(f'**City:** {company_data["city"]}')
cols[0].write(f'**Business Type:** {company_data["business_type"]}')


cols[1].markdown("""
    <div style="background-color: #005580; padding: 0.5px; border-radius: 10px; margin-bottom: 20px;">
        <h4 style="color: white; padding-left: 10px;">Financials</h4>
    </div>
""", unsafe_allow_html=True)

cols[1].write(f'**AUM:** £{company_data["aum"]:,.0f}')
cols[1].write(f'**Contributions:** £{company_data["contributions"]:,.0f}')
cols[1].write(f'**Annual Revenue:** £{company_data["annual_revenue"]:,.0f}')
cols[1].write(f'**Active Members:** {company_data["active_members"]:,.0f}')
cols[1].write(f'**Deferred Members:** {company_data["deferred_members"]:,.0f}')
cols[1].write(f'**Total Members:** {company_data["total_members"]:,.0f}')


cols[2].markdown("""
    <div style="background-color: #005580; padding: 0.5px; border-radius: 10px; margin-bottom: 20px;">
        <h4 style="color: white; padding-left: 10px;">Contact Details</h4>
    </div>
""", unsafe_allow_html=True)

cols[2].write(f'**Client Representative:** {company_data["client_representative"]}')
cols[2].write(f'**Contact Name:** {company_data["contact_name"]}')
cols[2].write(f'**Email:** {company_data["email"]}')
cols[2].write(f'**Website:** {company_data["website"]}')
cols[2].write(f'**Onboarding Date:** {company_data["onboarding_date"]}')


cols[3].markdown("""
    <div style="background-color: #005580; padding: 0.5px; border-radius: 10px; margin-bottom: 20px;">
        <h4 style="color: white; padding-left: 10px;">Description</h4>
    </div>
""", unsafe_allow_html=True)

cols[3].write(f'**Description:** {company_data["description"]}')


#------------------------- Data Table --------------------------------------#
data_container = st.container(border=True, height=1000)
data_container.subheader('Table')

search_cols = data_container.columns(2)

with search_cols[0].container():
    search_cols[0].subheader('Filter')
    filter_df = search_cols[0].selectbox('Filter by:', ['product_type', 'status', 'business_type', 'city'])
    filter_value = search_cols[0].selectbox('Select value:', df[filter_df].unique())
    filtered_df = df[df[filter_df] == filter_value]

with search_cols[1].container():
    search_cols[1].subheader('Search')
    search = search_cols[1].text_input('Search Company')
    search_results = filtered_df[filtered_df['company_name'].str.contains(search, case=False)]

if search:
    data_container.dataframe(search_results.style.format({
    'aum': '£{:,.0f}',
    'onboarding_year': lambda x: f"{x:.0f}",
    'contributions': '£{:,.0f}',
    'active_members': '{:,.0f}',
    'deferred_members': '{:,.0f}',
    'total_members': '{:,.0f}',
    'aum_bps': lambda x: f'{x * 100:,.2f}%',
    'additional_fee_bps': lambda x: f'{x * 100:,.2f}%',
    'onboarding_date': '{:%Y-%m-%d}',
    'annual_revenue': '£{:,.0f}'
    }), height=650, hide_index=True)
else:
    data_container.dataframe(filtered_df.style.format({
    'aum': '£{:,.0f}',
    'onboarding_year': lambda x: f"{x:.0f}",
    'contributions': '£{:,.0f}',
    'active_members': '{:,.0f}',
    'deferred_members': '{:,.0f}',
    'total_members': '{:,.0f}',
    'aum_bps': lambda x: f'{x * 100:,.2f}%',
    'additional_fee_bps': lambda x: f'{x * 100:,.2f}%',
    'onboarding_date': '{:%Y-%m-%d}',
    'annual_revenue': '£{:,.0f}'
    }), height=650, hide_index=True)
    























