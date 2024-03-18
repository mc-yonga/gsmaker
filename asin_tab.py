from superDev import *

def merge_data(frdate, todate, asin):
    st.session_state["merge_data"] = None
    st.session_state["asin_sheet_maker"] = None
    st.session_state["asin_result_df"] = None

    sqp = fetch_sqp(frdate, todate, asin)
    top_search_term = fetch_top_search_terms(frdate, todate, asin)
    impressions_share = fetch_impressions_share(frdate, todate, asin)

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
        total_df = pd.concat([df1, df2, df3], axis=1).reset_index()

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

        st.session_state["merge_data"] = total_df

def make_multiple_factors(total_df : pd.DataFrame):
    total_df['impressions_market_share'] = total_df.brand_impressions / total_df.market_impressions
    total_df['clicks_share'] = total_df.brand_clicks / total_df.market_clicks
    total_df['conversions_share'] = total_df.brand_conversions / total_df.market_conversions

    total_df['clicks_compe1'] = round(total_df.market_clicks * (total_df.asin_click_share_1 / 100))
    total_df["clicks_compe2"] = round(total_df.market_clicks * (total_df.asin_click_share_2 / 100))
    total_df["clicks_compe3"] = round(total_df.market_clicks * (total_df.asin_click_share_3 / 100))

    total_df["market_ctr"] = total_df.market_clicks / total_df.market_impressions
    total_df["brand_ctr"] = total_df.brand_clicks / total_df.brand_impressions
    total_df['ctr_gap'] = total_df.brand_ctr - total_df.market_ctr

    total_df["conversion_compe1"] = round(total_df.market_conversions * (total_df.asin_conversion_share_1 / 100))
    total_df["conversion_compe2"] = round(total_df.market_conversions * (total_df.asin_conversion_share_2 / 100))
    total_df["conversion_compe3"] = round(total_df.market_conversions * (total_df.asin_conversion_share_3 / 100))

    total_df["market_cvr"] = total_df.market_conversions / total_df.market_clicks
    total_df["brand_cvr"] = total_df.brand_conversions / total_df.brand_clicks
    total_df["cvr_compe1"] = total_df["conversion_compe1"] / total_df["clicks_compe1"]
    total_df["cvr_compe2"] = total_df["conversion_compe2"] / total_df["clicks_compe2"]
    total_df["cvr_compe3"] = total_df["conversion_compe3"] / total_df["clicks_compe3"]

    total_df["cvr_gap_1#"] = total_df["cvr_compe1"] - total_df.market_cvr
    total_df["cvr_gap_2#"] = total_df["cvr_compe2"] - total_df.market_cvr
    total_df["cvr_gap_3#"] = total_df["cvr_compe3"] - total_df.market_cvr

    total_df = total_df.drop(columns = ['asin_click_share_1', 'asin_click_share_2','asin_click_share_3','asin_conversion_share_1','asin_conversion_share_2','asin_conversion_share_3'])

    return total_df


def sheet_maker(frdate, todate, asin):
    merge_data(frdate, todate, asin)

    if st.session_state["merge_data"] is not None:
        result = make_multiple_factors(st.session_state["merge_data"])
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
        st.session_state["asin_sheet_maker"] = result


def groupby_maker(result):
    result_groupby = result.groupby('search_query')[
        ['search_query_volume', 'market_impressions', 'brand_impressions', 'market_clicks', 'brand_clicks',
         'market_conversions', 'brand_conversions',
         'ppc_impressions', 'ppc_clicks', 'ppc_orders_7days', 'ppc_sales_7days', 'ppc_spend',
         'clicks_compe1', 'clicks_compe2', 'clicks_compe3', 'conversion_compe1', 'conversion_compe2', 'conversion_compe3']].sum()

    result_groupby['impressions_market_share'] = round(
        result_groupby.brand_impressions / result_groupby.market_impressions, 4)
    result_groupby['market_ctr'] = round(result_groupby.market_clicks / result_groupby.market_impressions, 4)
    result_groupby['brand_ctr'] = round(result_groupby.brand_clicks / result_groupby.market_clicks)
    result_groupby['ctr_gap'] = result_groupby.brand_ctr - result_groupby.market_ctr
    result_groupby['brand_clicks_share'] = round(result_groupby.brand_clicks / result_groupby.market_clicks, 4)
    result_groupby['purchase_market_share'] = round(
        result_groupby.brand_conversions / result_groupby.market_conversions, 4)
    result_groupby['market_cvr'] = round(result_groupby.market_conversions / result_groupby.market_clicks, 4)
    result_groupby['brand_cvr'] = round(result_groupby.brand_conversions / result_groupby.brand_clicks, 4)
    result_groupby['cvr_compe1'] = round(result_groupby["conversion_compe1"] / result_groupby["clicks_compe1"], 4)
    result_groupby['cvr_compe2'] = round(result_groupby["conversion_compe2"] / result_groupby["clicks_compe2"], 4)
    result_groupby['cvr_compe3'] = round(result_groupby["conversion_compe3"] / result_groupby["clicks_compe3"], 4)
    result_groupby["cvr_gap_compe1"] = result_groupby["cvr_compe1"] - result_groupby["market_cvr"]
    result_groupby["cvr_gap_compe2"] = result_groupby["cvr_compe2"] - result_groupby["market_cvr"]
    result_groupby["cvr_gap_compe3"] = result_groupby["cvr_compe3"] - result_groupby["market_cvr"]
    result_groupby["cvr_gap"] = result_groupby["brand_cvr"] - result_groupby["market_cvr"]
    result_groupby["ppc_ctr"] = round(result_groupby["ppc_clicks"] / result_groupby["ppc_impressions"], 4)
    result_groupby["ppc_cvr"] = round(result_groupby["ppc_orders_7days"] / result_groupby["ppc_clicks"], 4)
    result_groupby["ppc_acos"] = round(result_groupby["ppc_spend"] / result_groupby["ppc_sales_7days"], 4)

    result_groupby = result_groupby[['search_query_volume','market_impressions', 'brand_impressions','impressions_market_share',
                                     'market_clicks','brand_clicks','clicks_compe1', 'clicks_compe2', 'clicks_compe3', 'market_ctr', 'brand_ctr', 'ctr_gap', 'brand_clicks_share',
                                     'market_conversions', 'brand_conversions', 'conversion_compe1', 'conversion_compe2', 'conversion_compe3', 'purchase_market_share',
                                     'market_cvr','brand_cvr', 'cvr_compe1', 'cvr_compe2', 'cvr_compe3', 'cvr_gap_compe1', 'cvr_gap_compe2', 'cvr_gap_compe3', 'cvr_gap',
                                     'ppc_sales_7days','ppc_spend', 'ppc_impressions', 'ppc_clicks', 'ppc_orders_7days', 'ppc_ctr', 'ppc_cvr', 'ppc_acos']]

    kw_data = result_groupby.groupby('search_query')['search_query_volume'].sum().reset_index()
    kw_data = kw_data.sort_values('search_query_volume', ascending=False)

    a, b = make_folder(kw_data, 3, 5, 3)

    folder_df = pd.concat([a, b])
    folder_df['folder'] = folder_df.main_folder + ' - ' + folder_df.sub_folder
    sq_list = folder_df.original_search_query.unique()
    kw_dict = {}
    for sq in sq_list:
        folder_config = folder_df[folder_df.original_search_query.apply(lambda x: sq in x)]['folder'].unique().tolist()
        kw_dict[sq] = folder_config
    result_groupby = result_groupby.reset_index()
    result_groupby['folder_config'] = result_groupby.search_query.apply(lambda x: kw_dict[x])

    return result_groupby.sort_values('search_query_volume', ascending=False)



if __name__ == '__main__':
    # frdate = '2024-01-01'
    # todate = '2024-12-31'
    # asin = 'B09G6BWNDP'
    # sheet_maker(frdate, todate, asin)
    create_table('zzz')
