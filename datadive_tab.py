from asin_tab import *
from superDev import *
import psycopg2

def connect_to_db():
    conn = psycopg2.connect(
        host="aws-0-us-west-1.pooler.supabase.com",
        port="5432",
        dbname="postgres",
        user="postgres.ozghbebrbzdxraywzuuk",
        password="superDev1!@#$%^&",
    )
    return conn

def fetch_sqp_by_keyword_list(frdate, todate, keyword_list):

    query = f"""
    SELECT
        date,
        search_query,
        search_query_volume,
        market_impressions,
        asin_impressions as brand_impressions,
        market_clicks,
        asin_clicks as brand_clicks,
        market_conversions,
        asin_conversions as brand_conversions,
    FROM `c25r-master-db.fiverock.search_query_performance_asin`
    where
        date >= '{frdate}' and
        date <= '{todate}' and
        search_query in {tuple(keyword_list)}
    order by date asc
    """

    resp = query_bq(query)
    return resp

def fetch_top_search_terms_by_keywords(frdate, todate, keyword_list):

    query = f"""
    SELECT 
        date, 
        search_term as search_query, 
        asin_click_share_1, 
        asin_click_share_2,
        asin_click_share_3, 
        asin_conversion_share_1, 
        asin_conversion_share_2, 
        asin_conversion_share_3
    FROM `c25r-master-db.fiverock.all_top_search_terms`
    WHERE
        search_term in {tuple(keyword_list)} and
        date >= '{frdate}' and
        date <= '{todate}'
    order by date asc
    """

    resp = query_bq(query)
    return resp

def fetch_impressions_share_by_keywords(frdate, todate, keyword_list):
    conn = connect_to_db()
    cursor = conn.cursor()

    query = f"""
    WITH campaign_list AS(
        SELECT DISTINCT CAST(campaign_id AS VARCHAR)
        FROM amazon_ads.search_term_ad_keyword_report
        WHERE 
            search_term in {tuple(keyword_list)}
        )
        
    SELECT ch.name
    FROM amazon_ads.campaign_history ch
    JOIN campaign_list  cl ON ch.id = cl.campaign_id
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    campaign_list = []
    for row in rows:
        campaign_list.append(row[0])


    query2 = f"""
        SELECT 
        date, 
        customer_search_term as search_query,
        targeting,
        match_type, 
        campaign_name,
        ad_group_name,
        impressions as ppc_impressions,
        clicks as ppc_clicks,
        _7_day_total_orders____ as ppc_orders_7days,
        _7_day_total_sales as ppc_sales_7days,
        spend as ppc_spend
    FROM `c25r-master-db.fiverock.impression_share` 
    WHERE
        campaign_name in {tuple(campaign_list)} and
        date >= '{frdate}' and
        date <= '{todate}'

    order by date asc
    """

    resp = query_bq(query2)
    return resp

def merge_data_by_datadive(frdate, todate, keyword_list):
    st.session_state["merge_data_by_datadive"] = None
    st.session_state["dive_sheet_maker"] = None
    st.session_state["asin_result_df"] = None

    sqp = fetch_sqp_by_keyword_list(frdate, todate, keyword_list)
    top_search_term = fetch_top_search_terms_by_keywords(frdate, todate, keyword_list)
    impressions_share = fetch_impressions_share_by_keywords(frdate, todate, keyword_list)

    if len(sqp) == 0 or len(top_search_term) == 0 or len(impressions_share) == 0:
        if len(sqp) == 0:
            st.error(f'Search Query Performance 데이터가 없습니다')
        elif len(top_search_term) == 0:
            st.error(f'경쟁사 데이터(top_search_term)가 없습니다')
        elif len(impressions_share) == 0:
            st.error('PPC Performance(impressions_share) 데이터가 없습니다. ')
    else:
        top_search_term_weekly = []
        impressions_share_weekly = []
        sqp_date_list = sorted(sqp.date.unique())

        for sqp_date in sqp_date_list:
            todate = sqp_date
            frdate = todate - datetime.timedelta(days=6)

            tst_cond = (frdate <= top_search_term.date) & (top_search_term.date <= todate)
            top_search_term_ = top_search_term[tst_cond].groupby('search_query')[
                ['asin_click_share_1', 'asin_click_share_2',
                 'asin_click_share_3', 'asin_conversion_share_1',
                 'asin_conversion_share_2', 'asin_conversion_share_3']].mean().reset_index()

            top_search_term_['date'] = todate
            top_search_term_ = top_search_term_[[top_search_term_.columns[-1]] + list(top_search_term_.columns[:-1])]
            top_search_term_weekly.append(top_search_term_)

            is_cond = (frdate <= impressions_share.date) & (impressions_share.date <= todate)
            impressions_share_ = impressions_share[is_cond].groupby('search_query')[
                ['ppc_impressions', 'ppc_clicks', 'ppc_orders_7days', 'ppc_sales_7days', 'ppc_spend']].sum().reset_index()
            impressions_share_['date'] = todate
            impressions_share_weekly.append(impressions_share_)

        top_search_term_weekly = pd.concat(top_search_term_weekly)
        impressions_share_weekly = pd.concat(impressions_share_weekly)
        df1 = sqp.set_index(['date', 'search_query'])
        df2 = top_search_term_weekly.set_index(['date', 'search_query'])
        df3 = impressions_share_weekly.set_index(['date', 'search_query'])
        # total_df = pd.concat([df1, df2, df3], axis=1).reset_index()
        total_df = df1.join([df2, df3], how = 'outer').reset_index()

        for row in total_df.itertuples():
            todate = row.date
            frdate = todate - datetime.timedelta(days=6)
            search_query = row.search_query

            cond = (impressions_share.date >= frdate) & (impressions_share.date <= todate) & (
                        impressions_share.search_query == search_query)
            campaigns = impressions_share[cond]['campaign_name'].unique().tolist()
            ad_groups = impressions_share[cond]['ad_group_name'].unique().tolist()
            campaigns = '+'.join(campaigns)
            ad_groups = '+'.join(ad_groups)

            total_df.loc[(total_df.date >= frdate) & (total_df.date <= todate) & (
                        total_df.search_query == search_query), 'campaigns'] = campaigns
            total_df.loc[(total_df.date >= frdate) & (total_df.date <= todate) & (
                        total_df.search_query == search_query), 'ad_groups'] = ad_groups

        total_df['campaigns'] = total_df.campaigns.apply(lambda x: x.split('+'))
        total_df['ad_groups'] = total_df.ad_groups.apply(lambda x: x.split('+'))

        st.session_state["merge_data_by_datadive"] = total_df


def sheet_maker_by_datadive(frdate, todate, keyword_list):
    merge_data_by_datadive(frdate, todate, keyword_list)

    if st.session_state["merge_data_by_datadive"] is not None:
        result = make_multiple_factors(st.session_state["merge_data_by_datadive"])
        kw_data = result.groupby('search_query')['search_query_volume'].sum().reset_index()
        kw_data = kw_data.sort_values('search_query_volume', ascending=False)

        a, b = make_folder(kw_data, 3, 5, 3)

        folder_df = pd.concat([a, b])
        folder_df['folder'] = folder_df.main_folder + ' - ' + folder_df.sub_folder
        sq_list = folder_df.original_search_query.unique()
        kw_dict = {}
        for sq in sq_list:
            folder_config = folder_df[folder_df.original_search_query.apply(lambda x: sq in x)]['folder'].unique().tolist()
            kw_dict[sq] = folder_config

        result['folder_config'] = result.search_query.apply(lambda x: kw_dict[x])
        st.session_state["dive_sheet_maker"] = result



