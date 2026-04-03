import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="Cortex AI Cost Monitor", layout="wide")
st.title("Cortex AI Cost Monitor")

with st.sidebar:
    st.header("Filters")
    timezone = st.selectbox("Timezone", [
        "UTC", "Asia/Singapore", "US/Eastern", "US/Pacific",
        "Europe/London", "Australia/Sydney"
    ], index=1)
    date_range = st.date_input(
        "Date Range",
        value=(pd.Timestamp.now() - pd.Timedelta(days=30), pd.Timestamp.now())
    )
    if len(date_range) == 2:
        s = date_range[0].strftime('%Y-%m-%d')
        e = date_range[1].strftime('%Y-%m-%d')
    else:
        s = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        e = pd.Timestamp.now().strftime('%Y-%m-%d')

# Standardized Timezone Conversions (snowflake stores as LTZ)
tz_convert_start = f"convert_timezone('{timezone}', start_time)::date"
tz_convert_usage = f"convert_timezone('{timezone}', usage_time)::date"
tz_convert_pt    = f"convert_timezone('{timezone}', interval_start_time)::date"

###########################################################################
# MASTER CTEs: DEFINING THE SOURCE OF TRUTH ONCE
###########################################################################

# 1. CORTEX FUNCTIONS MASTER (Consolidating the 3 legacy/current views)
cortex_functions_master_cte = f"""
cortex_func_raw as (
    select usage_time as raw_ts, user_id, model_name, function_name, token_credits as credits, tokens 
    from snowflake.account_usage.cortex_aisql_usage_history
    where {tz_convert_usage} between '{s}' and '{e}'
    union all
    select start_time as raw_ts, user_id, model_name, function_name, credits, 0 as tokens 
    from snowflake.account_usage.cortex_ai_functions_usage_history
    where {tz_convert_start} between '{s}' and '{e}'
    union all
    select start_time as raw_ts, null as user_id, model_name, function_name, token_credits as credits, tokens 
    from snowflake.account_usage.cortex_functions_usage_history
    where {tz_convert_start} between '{s}' and '{e}'
),
cortex_functions_consolidated as (
    select 
        convert_timezone('{timezone}', raw_ts)::date as start_date,
        user_id,
        model_name, 
        function_name, 
        sum(credits) as total_credits, 
        sum(tokens) as total_tokens
    from cortex_func_raw
    group by all
)
"""

# 2. REST API MASTER (Applying custom pricing logic)
# 2. REST API MASTER (Fixed Logic)
rest_api_calc_cte = f"""
raw_data_rest_api as (
    select model_name, tokens, start_time, user_id, request_id, 
        coalesce(tokens_granular['input']::numeric,0) as input_token,
        coalesce(tokens_granular['output']::numeric,0) as output_token,
        coalesce(tokens_granular['cache_read_input']::numeric,0) as cache_read_token,
        coalesce(tokens_granular['cache_write_input']::numeric,0) as cache_write_token,
        tokens - input_token - output_token - cache_read_token - cache_write_token as other_token
    from snowflake.account_usage.cortex_rest_api_usage_history
    where {tz_convert_start} between '{s}' and '{e}'
),
unpivoted_tokens as (
    select model_name, start_time, user_id, request_id, 'input' as token_type, input_token as token_count from raw_data_rest_api
    union all 
    select model_name, start_time, user_id, request_id, 'output', output_token from raw_data_rest_api
    union all 
    select model_name, start_time, user_id, request_id, 'cache_read', cache_read_token from raw_data_rest_api
    union all 
    select model_name, start_time, user_id, request_id, 'cache_write', cache_write_token from raw_data_rest_api
),
-- STEP 1: Define the final name first
naming_fix as (
    select *,
        case when model_name = 'default' then 'claude-sonnet-4-5' else model_name end as model_name_final
    from unpivoted_tokens
),
-- STEP 2: Use the final name for pricing
calculations_rest_api_tmp as (
    select 
        start_time, user_id, request_id, model_name_final, token_count,
        case 
            when token_type = 'input' then 
                case 
                    when model_name_final like 'llama%' then 2.4 
                    when model_name_final like 'claude%sonnet%' then 3.3
                    when model_name_final like 'claude%opus%' then 15
                    else 3
                end 
            when token_type = 'output' then 
                case 
                    when model_name_final like 'llama%' then 2.4
                    when model_name_final like 'claude%sonnet%' then 16
                    when model_name_final like 'claude%opus%' then 75
                    else 10
                end 
            when token_type = 'cache_read' then 
                case 
                    when model_name_final like 'llama%' then 0
                    when model_name_final like 'claude%sonnet%' then 0.33
                    when model_name_final like 'claude%opus%' then 0.55
                    else 0.5
                end
            when token_type = 'cache_write' then 
                case 
                    when model_name_final like 'llama%' then 0
                    when model_name_final like 'claude%sonnet%' then 4.13
                    when model_name_final like 'claude%opus%' then 18.75
                    else 10
                end                  
            else 0
        end as usd_per_million_token
    from naming_fix        
),
calculations_rest_api as (
    select
        convert_timezone('{timezone}', start_time)::date as start_date, 
        user_id,
        model_name_final, 
        request_id,
        sum(token_count) as total_tokens,
        sum(token_count / 1000000 * usd_per_million_token) as total_credits
    from calculations_rest_api_tmp
    group by all
)
"""

# 3. CORTEX CODE MASTER CTE

cortex_code_calc_cte = f"""
    select 'Cortex Code (Snowsight)' as service, user_id, round(sum(token_credits), 2) as total_credits 
    from snowflake.account_usage.cortex_code_snowsight_usage_history 
    where {tz_convert_usage} between '{s}' and '{e}'
    group by all
    union all
    select 'Cortex Code (CLI)' as service, user_id, round(sum(token_credits), 2) as total_credits 
    from snowflake.account_usage.cortex_code_cli_usage_history 
    where {tz_convert_usage} between '{s}' and '{e}'
    group by all
"""

#4 CORTEX AGENT MASTER CTE

cortex_agent_calc_cte = f"""
    select 
        user_id, 
        round(sum(token_credits), 2) as total_credits, 
        count(distinct request_id) as total_requests 
    from snowflake.account_usage.cortex_agent_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
    """

# DOCUMENT PROCESSING MASTER CTE

document_process_calc_cte = f"""
    select 
        null as user_id, 
        round(sum(credits_used), 2) as total_credits, 
        sum(page_count) as total_pages 
    from snowflake.account_usage.cortex_document_processing_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
    """

#############################################
# OVERVIEW: ALL AI COSTS SUMMARY
#############################################
st.markdown("## AI Cost Overview")

overview_sql = f"""with 
{cortex_functions_master_cte}, 
{rest_api_calc_cte},
all_costs as (
    select 'Cortex AI Functions' as service, user_id, round(sum(total_credits),2) as total_credits 
    from cortex_functions_consolidated
    group by all
    
    union all
    select 'Cortex Analyst', null as user_id, round(sum(credits), 2) 
    from snowflake.account_usage.cortex_analyst_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
    
    union all
    select 'Cortex Search', null as user_id, round(sum(credits), 2) 
    from snowflake.account_usage.cortex_search_serving_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
    
    union all
    select 'Document Processing', user_id, round(sum(total_credits), 2) 
    from ({document_process_calc_cte})
    group by all
    
    union all

    {cortex_code_calc_cte}
    
    union all
    select 'Cortex Agents', user_id, round(sum(total_credits), 2) 
    from ({cortex_agent_calc_cte})
    group by all
    
    union all
    select 'Fine-Tuning', null as user_id, round(sum(token_credits), 2) 
    from snowflake.account_usage.cortex_fine_tuning_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
    
    union all
    select 'Cortex REST API', user_id, round(sum(total_credits), 2) 
    from calculations_rest_api
    group by all
    
    union all
    select 'Provisioned Throughput', null as user_id, round(sum(ptu_credits), 2) 
    from snowflake.account_usage.cortex_provisioned_throughput_usage_history 
    where {tz_convert_pt} between '{s}' and '{e}'
    group by all
    
    union all
    select 'Snowflake Intelligence', user_id, round(sum(token_credits), 2) 
    from snowflake.account_usage.snowflake_intelligence_usage_history 
    where {tz_convert_start} between '{s}' and '{e}'
    group by all
)
select 
    service,
    user_id,
    coalesce(total_credits, 0) as total_credits 
from all_costs 
where total_credits > 0 
order by total_credits desc
"""

try:
    overview_df = session.sql(overview_sql).to_pandas()
    if not overview_df.empty:
        grand_total = overview_df['TOTAL_CREDITS'].sum()
        st.metric("Total AI Credits", "{:,.2f}".format(grand_total))
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Credits by Service")
            service_chart_data = overview_df.groupby('SERVICE')['TOTAL_CREDITS'].sum().sort_values(ascending=False)
            st.bar_chart(service_chart_data, horizontal=True, sort='-TOTAL_CREDITS')
            
        with col2:
            st.dataframe(service_chart_data.reset_index(), use_container_width=True, hide_index=True)
except Exception as ex:
    st.warning(f"Could not load overview: {ex}")

#############################################
# CORTEX AI FUNCTIONS (CONSOLIDATED)
#############################################
st.markdown("---")
st.markdown("## Cortex AI Functions & SQL")

func_detail_sql = f"with {cortex_functions_master_cte} select * from cortex_functions_consolidated"

try:
    func_df = session.sql(func_detail_sql).to_pandas()
    col1, col2 = st.columns(2)
    col1.metric("Total Inference Credits", "{:,.2f}".format(func_df['TOTAL_CREDITS'].sum()))
    col2.metric("Total Inference Tokens", "{:,.0f}".format(func_df['TOTAL_TOKENS'].sum()))

    st.markdown("#### Credits by Model")
    model_chart_data = func_df.groupby('MODEL_NAME')['TOTAL_CREDITS'].sum().sort_values(ascending=False)
    st.bar_chart(model_chart_data, horizontal=True, sort='-TOTAL_CREDITS')
    
    st.dataframe(func_df, use_container_width=True, hide_index=True)
except Exception as ex:
    st.warning(f"Error loading Functions: {ex}")

#############################################
# CORTEX CODE
#############################################
st.markdown("---")
st.markdown("## Cortex Code")

try:
    code_df = session.sql(cortex_code_calc_cte).to_pandas()
    st.metric("Total Cortex Code Credits", "{:,.2f}".format(code_df['TOTAL_CREDITS'].sum()))
    st.dataframe(code_df, use_container_width=True, hide_index=True)
except Exception as ex:
    st.warning(f"Error loading Code: {ex}")

#############################################
# CORTEX AGENTS
#############################################
st.markdown("---")
st.markdown("## Cortex Agents")
try:
    agents_df = session.sql(cortex_agent_calc_cte).to_pandas()
    c1, c2 = st.columns(2)
    c1.metric("Agent Credits", "{:,.2f}".format(agents_df['TOTAL_CREDITS'].sum()))
    c2.metric("Agent Requests", "{:,.0f}".format(agents_df['TOTAL_REQUESTS'].sum()))
except Exception as ex: st.warning(ex)

#############################################
# DOCUMENT PROCESSING
#############################################
st.markdown("---")
st.markdown("## Document Processing")
try:
    doc_df = session.sql(document_process_calc_cte).to_pandas()
    c1, c2 = st.columns(2)
    c1.metric("Doc Processing Credits", "{:,.2f}".format(doc_df['TOTAL_CREDITS'].sum()))
    c2.metric("Doc Processing Pages", "{:,.0f}".format(doc_df['TOTAL_PAGES'].sum()))
except Exception as ex: st.warning(ex)

#############################################
# CORTEX REST API
#############################################
st.markdown("---")
st.markdown("## Cortex REST API")
try:
    rest_sql = f"""
    with {rest_api_calc_cte} 
        select 
            model_name_final as model_name, 
            sum(total_tokens) as tokens, 
            sum(total_credits) as credits, 
            count(distinct request_id) as requests 
        from calculations_rest_api 
        group by 1"""
    rest_df = session.sql(rest_sql).to_pandas()
    c1, c2 = st.columns(2)
    c1.metric("Calculated REST Credits", "{:,.2f}".format(rest_df['CREDITS'].sum()))
    c2.metric("Rquests", "{:,.0f}".format(rest_df['REQUESTS'].sum()))
    st.warning("Please note that Cortex REST API is not having automatic credit info from Snowflake, this is estimated cost based on https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf")
    st.dataframe(rest_df, use_container_width=True, hide_index=True)
except Exception as ex: st.warning(ex)

#############################################
# TOP USER CONSUMPTION
#############################################
st.markdown("---")
st.header("👤 Top User Consumption")

# This query joins your overview data with the actual User names
user_display_sql = f"""
with all_usage as ({overview_sql})
select
    usage.user_id,
    --coalesce(u.login_name, try_to_varchar(usage.user_id), 'System/Service') as user_name,
    usage.service,
    sum(usage.total_credits) as credits
from all_usage usage
--left join snowflake.account_usage.users u on usage.user_id = try_to_varchar(u.user_id)
group by all
order by 3 desc
"""

try:
    user_df = session.sql(user_display_sql).to_pandas()
    if not user_df.empty:
        # Create a stacked bar chart: Users on X-axis, Services as colors
        user_pivot = user_df.pivot(index='USER_ID', columns='SERVICE', values='CREDITS').fillna(0)
        # st.dataframe(user_pivot)
        st.markdown("#### Credit Spend by User and Feature")
        st.bar_chart(user_pivot, horizontal=True)
        
        st.markdown("#### Top Spender Details")
        st.dataframe(user_df, use_container_width=True, hide_index=True)
except Exception as ex:
    st.error(f"Error loading User breakdown: {ex}")
st.divider()


st.info("Version 5.3 — DRY refactor and added userID | By: Cortex Code + Gemini + Grumpy human after correcting all the messy Snowflake Views | 2026-04-03")

st.balloons()