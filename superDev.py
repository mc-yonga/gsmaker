from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import psycopg2
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import stopwords
import numpy as np
import datetime
import streamlit as st

lemmatizer = WordNetLemmatizer()
nltk.download("wordnet")
nltk.download("stopwords")
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
try:
    stop_words = set(stopwords.words("english"))
except AttributeError:
    pass


pd.set_option('display.max_columns', None)

secret = {
  "type": "service_account",
  "project_id": "c25r-master-db",
  "private_key_id": "b15ee39d2f34f6536335af96c111d3245741065b",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDR3nDGeJWKZlGK\n8TmtyPhCT+SilnMAihWDzQqmIbTEDf30izOU1WNwvt6nFa6OtJLVqY0TfV54yUYW\ntC+OjQDNwkHWk+R0S+zc94XlCsm5LwvDPa5W6kywlEbQkk63lgwo8VLaU4fQUzpo\noeQQlCKC3Ioka0I+6LoFh4ShFgjfmFYwxLEDXAlmVrnisKIPKOO1JTQH1xrg/iTF\nKAGZ0JT33NqDRt4754zYE5/zzqekVpDTItj/Xv6NzwiPXtBIPybOBxuK3NBvJVu2\n0LlNFJqgLi9RMo4uTK4Ho6FdGDxMscgfW/q5t3UNGhasiK7uzolHCkLfHjyl2rW/\n5suXHIxRAgMBAAECgf8Zpww3NkYRGgXy6x7Bv0RD1sAmSigvHgcYioI4DFFW0DRa\nen6zkoxfxkow4irpW+kWyhgUaclCZQJ22U920oVmoSKrDSi/jl2az3vuTdiaahsQ\noll3sOvu1DD+YuPRojrvm4/PtVDEhhJ9okwzj+fSrGJg++X9CoqyYlOJMnL4jCLz\nXFNzK2Wc/vbLkZfhDWf+pjq50BP4o6ofyCT++p8B6e/ot77W6y7iDFjkwMTO+txC\nxkHdjU2NZUGw1km4TLJTE+wXzbPJ0zq1biBFathR+zAi1ZecqcUFA/XHKugrdF9k\n9NgD3d6ZULYWvfXN3p4CAB7RaKhqc1ik6fSAi4kCgYEA8JPjb3ljqzne42nGAdXi\nfxa5iK+UuYAJLXuhCW/dpxh8eDq43MI9Zt7d93TOIvOgXtjmanv4xmsWcdA3c8K+\nfdtnLQfYW2TsHOfb0bjQLq2aSgGw+5vm0pkwj0LV4ULIOCsab7HrJF3uLTvcEfYV\nST8j5Q9xTvgnKlHZ/gBp5hkCgYEA31KXaJXwE5gKH8P7j0QDoe7BGcnEfxj1N/Gt\nrUij6N6/8TJ+bLlt9990iqJQqQB86MOdbxLD8mzFvexn9iHubJ6RpCqWUuHGkK74\n+7+LZZKCGjXB1dLHorbFDEr7pPoAZTdxtCs7XOWQUdoL/8oNaJNhDoIOnfvl5VQP\nZk7FbvkCgYEAkAA1lrgWTJtrKrxZZSfdVy0HCSXv48kbtTnW/osTJb2mY2Di3mD1\n1+l2+3PTH2CskZlK8loaYsoeuSlkx8m9tB/r9ixH+QzDt9mg3ju9gPMw8zNn+HMt\nCsnfIyFiXF9Y8SX7wPfCRBZlRnYaGDYwL5O3rJg9voMTDmXIEh21RaECgYEA1IRm\nLVKyGhNPXxdDpvxUcJ8iB4ZohYKcqNZGLma4BH0lL0Sb5p83NPDimKMKGymptF+i\ny/aRnXtBWhFEhYeYuqjTc+RLFShhq4G20utenhQj1wldIjvpWsCPF7mraz21bpyq\niYeygB69jgbv8ES5KdFGRWYivtjd17R8yyEe5RECgYEA7KMdhtHkzBLvTEs56aoT\ngNGdHKNxF76EBhlFBO50pDt1vnisv838NF4iblnuNpnBTrLfUctX3NuDTASnrqyu\ninK6zv9dkfvOnPwFvoUIQY1eGRID+7lJvKLa+T4QfZinIE3A+y/Q0sk2ZVIM7ytq\nPPQLCgi0U55EzZXSTyOQYRo=\n-----END PRIVATE KEY-----\n",
  "client_email": "data-sharing@c25r-master-db.iam.gserviceaccount.com",
  "client_id": "110229429866925312451",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/data-sharing%40c25r-master-db.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


def get_sa_cred(scopes=["https://www.googleapis.com/auth/cloud-platform"]):

    credentials = service_account.Credentials.from_service_account_info(
        secret,  scopes=scopes)
    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)

    return client, credentials

def connect_to_db():
    conn = psycopg2.connect(
        host="aws-0-ap-northeast-2.pooler.supabase.com",
        port="5432",
        dbname="postgres",
        user="postgres.yjolqcetitlpjbwwxnly",
        password="superDev1!@#$%^&",
    )
    return conn

def query_bq(query: str) -> pd.DataFrame:
    """
    This function queries BigQuery and returns a dataframe.
    Args:
        query (str): the query to run
        client (bigquery.Client): the client to use to run the query
    Returns:
        df (pandas.DataFrame): dataframe with the data from the query
    """
    client, credentials = get_sa_cred()
    df = client.query(query).to_dataframe()
    return df


def fetch_sqp(frdate, todate, asin):
    query = f"""
    SELECT 
        date, 
        asin, 
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
        asin = '{asin}' and
        date >= '{frdate}' and
        date <= '{todate}'      
    order by date asc 
    """

    resp = query_bq(query)
    return resp

def fetch_top_search_terms(frdate, todate, asin):
    query = f"""
    WITH sq_list AS (
        SELECT search_query
        FROM `c25r-master-db.fiverock.search_query_performance_asin`
        where 
            asin = '{asin}' and
            date >= '{frdate}' and
            date <= '{todate}'      
        order by date asc 
    )

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
        search_term in (SELECT search_query from sq_list) and
        date >= '{frdate}' and
        date <= '{todate}'
    order by date asc
    """

    resp = query_bq(query)
    return resp


def fetch_impressions_share(frdate, todate, asin):
    query = f"""
    WITH total_campaign_list AS (
        SELECT `Campaign Name`
        FROM `c25r-master-db.scale_insights.campaign_asin_mapping_temp`
        WHERE 
            ASIN = '{asin}'
    )

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
        campaign_name in (SELECT `Campaign Name` from total_campaign_list) and
        date >= '{frdate}' and
        date <= '{todate}'

    order by date asc
    """
    resp = query_bq(query)
    return resp


def fetch_rank(frdate, todate, asin):
    query = f"""

    WITH sq_list AS (
        SELECT search_query
        FROM `c25r-master-db.fiverock.search_query_performance_asin`
        where 
            asin = '{asin}' and
            date >= '{frdate}' and
            date <= '{todate}'      
        order by date asc 
    )

    SELECT
        result_request_metadata_created_at as date, 
        request_search_term as search_query, 
        result_search_results_position as position, 
        result_search_results_page as page
    FROM `c25r-master-db.keyword_rank.product_console_rank`
    WHERE
        request_search_term in (SELECT search_query from sq_list) and
        result_request_metadata_created_at >= '{frdate}' and
        result_request_metadata_created_at <= '{todate}'
    order by result_request_metadata_created_at asc
    """

    resp = query_bq(query)
    return resp

def connect_to_db():
    conn = psycopg2.connect(
        host="aws-0-ap-northeast-2.pooler.supabase.com",
        port="5432",
        dbname="postgres",
        user="postgres.yjolqcetitlpjbwwxnly",
        password="superDev1!@#$%^&",
    )
    return conn

def create_table(name):
    conn = connect_to_db()
    cursor = conn.cursor()
    query = f"""
    CREATE TABLE "{name}" (
        search_query text,
        search_query_volume integer,
        market_impressions integer,
        brand_impressions integer,
        impressions_market_share integer,
        market_clicks integer,
        brand_clicks integer,
        "#1 clicks" integer,
        "#2 clicks" integer,
        "#3 clicks" integer,
        market_ctr integer,
        brand_ctr integer,
        ctr_gap integer,
        brand_clicks_share integer,
        market_conversions integer,
        brand_Conversions integer,
        "#1 conversion" integer,
        "#2 conversion" integer,
        "#3 conversion" integer,
        purchase_market_share integer,
        market_cvr integer,
        brand_cvr integer,
        "#1 cvr" integer,
        "#2 cvr" integer,
        "#3 cvr" integer,
        "#1 cvr gap" integer,
        "#2 cvr gap" integer,
        "#3 cvr gap" integer,
        cvr_gap integer,
        ppc_sales_7days integer,
        ppc_spend integer,
        ppc_impressions integer,
        ppc_clicks integer,
        ppc_orders_7days integer,
        ppc_ctr integer,
        ppc_cvr integer,
        ppc_acos integer,
        folder_config text[]
           );
        """
    "Search Query Tag"
    # text ARRAY
    cursor.execute(query)
    conn.commit()
    print(f"{name} 테이블 추가 완료")
    conn.close()

def insert_data(name, data):  # 모든 문자열 열에 대해 replace 수행
    conn = connect_to_db()
    cursor = conn.cursor()
    str_columns = data.select_dtypes(include=["object"])
    data[str_columns.columns] = str_columns.applymap(lambda x: x.replace("'", "''"))

    # 데이터 일괄 처리를 위한 쿼리 생성
    values = []
    for row in data.itertuples():
        values.append(
            f"('{row.search_query}', '{row.search_query_volume}', '{row.market_impressions}', '{row.brand_impressions}', {row.impressions_market_share}, '{row.market_clicks}',"
            f" '{row.brand_clicks}', '{row._7}', '{row._8}', '{row._9}', '{row.market_ctr}', '{row.brand_ctr}', '{row.ctr_gap}', '{row.brand_clicks_share}', '{row.market_conve}' )"
        )

    query = f"""INSERT INTO "{name}" (category, main_folder, sub_folder, search_query, search_volume, original_search_query, priority, kw_push, phase, difficulty_level) VALUES {', '.join(values)}"""

    try:
        cursor.execute(query)
        conn.commit()
        print(f"{name} 태그 데이터 저장 완료")
    except Exception as e:
        print(f"데이터 삽입 중 오류 발생: {e}")
        conn.rollback()
    conn.close()


def kw_normalization(data):
    data["kw_split"] = data.search_query.apply(lambda x: x.split())
    data["kw_split"] = data.kw_split.apply(
        lambda x: [
            kw
            for kw in x
            if any(char.isalpha() or char.isdigit() for char in kw)
            and kw not in stop_words  # 불용어와 기호 제거
        ]
    )

    data["kw_split_lemma"] = data.kw_split.apply(
        lambda x: [
            kw if pos_tag([kw])[0][1] == "NN" else lemmatizer.lemmatize(kw) for kw in x
        ]
    )
    data.rename(columns={"search_query": "original_search_query"}, inplace=True)
    data["search_query"] = data.kw_split_lemma.apply(lambda x: " ".join(x))

    return data

def make_folder(data, word_count, main_keywords_huddle, sub_keywords_huddle):
    print("Maker folder Start !!")
    data = data.sort_values("search_query_volume", ascending=False)

    data["kw_split"] = data.search_query.apply(lambda x: x.split())
    data.index = np.arange(len(data))

    data = kw_normalization(data)  # 써치쿼리를 nomarlization
    sq_volumes = list(zip(data.search_query, data.search_query_volume))

    total_folder = {}
    while True:
        top_folder_name = sq_volumes[0][0]
        total_sub_folder_df = data[
            data.search_query.apply(
                lambda x : top_folder_name in x
            )
        ]
        sub_folder_keywords = total_sub_folder_df.search_query.tolist()

        total_sub_folder = {}
        while True:
            sub_folder_name = sub_folder_keywords[0]
            sub_folder_name_split = sub_folder_name.split()
            sub_total_df = total_sub_folder_df[
                total_sub_folder_df.search_query.apply(
                    lambda x: sub_folder_name in x
                )
            ]

            total_sub_keywords = sub_total_df.search_query.tolist()

            total_sub_folder[sub_folder_name] = sub_total_df
            if len(sub_folder_name_split) > 1:
                sub_folder_keywords = list(
                    filter(lambda x: x not in total_sub_keywords, sub_folder_keywords)
                )
            else:
                sub_folder_keywords.remove(sub_folder_name)

            # print(
            #     f'top folder name >> {top_folder_name}, sub_folder_name >> {sub_folder_name}, sub keyword 갯수 >> {len(sub_total_df)}, sub_folder갯수 >> {len(sub_folder_keywords)}')
            if len(sub_folder_keywords) == 0:
                break

        total_folder[top_folder_name] = total_sub_folder
        sq_volumes = list(filter(lambda x: top_folder_name not in x[0], sq_volumes))

        if len(sq_volumes) == 0:
            break

    ### line 123 ~ 143 / top folder uncate 만들기 ###
    uncate_main_folders = []

    for main_folder in total_folder.keys():
        sub_folders = total_folder[main_folder].keys()
        sub_folder_dfs = []
        for sub_folder in sub_folders:
            sub_folder_df = total_folder[main_folder][sub_folder]
            sub_folder_dfs.append(sub_folder_df)

        sub_folder_dfs = pd.concat(sub_folder_dfs)
        sub_folder_dfs = sub_folder_dfs.drop_duplicates(subset="search_query")
        # print(f'써브폴더 dfs 갯수 >> {len(sub_folder_dfs)}, 메인키워드 허들 갯수 >> {main_keywords_huddle}, 타입1 >> {type(len(sub_folder_dfs))}, 타입2 >> {type(main_keywords_huddle)}')
        if (
            len(sub_folder_dfs) < main_keywords_huddle
            or len(main_folder.split()) > word_count
        ):
            uncate_main_folders.append(main_folder)

    uncate_dfs = {}
    for uncate_folder in uncate_main_folders:
        uncate_dfs[uncate_folder] = total_folder[uncate_folder]
        del total_folder[uncate_folder]

    total_folder["uncategorized"] = uncate_dfs

    ### line 145 ~ 161  / sub_folder keyword 의 수가 적으면 언카테고리로 이동시킴 ###
    for folder_name in total_folder.keys():
        if folder_name == "uncategorized":
            continue
        sub_folder_list = total_folder[folder_name].keys()
        sub_uncate_dfs = []
        remove_sub_folders = []
        for sub_folder in sub_folder_list:
            sub_folder_df = total_folder[folder_name][sub_folder]
            if len(sub_folder_df) < sub_keywords_huddle:
                remove_sub_folders.append(sub_folder)
                sub_uncate_dfs.append(sub_folder_df)
                # print(f'sub_folder >> {sub_folder}, {len(sub_folder_df)} 언카테고리로 이동')
        if len(sub_uncate_dfs) > 0:
            total_folder[folder_name]["uncategorized"] = pd.concat(sub_uncate_dfs)
            for remove_folder_name in remove_sub_folders:
                del total_folder[folder_name][remove_folder_name]

    ### line 96 ~ 121 / 폴더명이 긴 폴더를 >> 폴더명이 짧은 폴더로 합치기 위한 코드 ###
    sorted_total_folder_names = sorted(
        list(total_folder.keys()), key=len
    )  # 폴더이름이 짧은게 위로 올라오도록 정렬

    for folder_name in sorted_total_folder_names:
        if folder_name not in total_folder.keys():
            # print(folder_name, '폴더는 이미 제거됨')
            continue

        merge_folder_names = list(filter(lambda x: folder_name in x and folder_name != x, sorted_total_folder_names))

        if len(merge_folder_names) >= 1:
            for merge_folder_name in merge_folder_names:
                if (
                    merge_folder_name in total_folder.keys()
                ):  ### 토탈폴더에 지워야 될 폴더가 있으면 폴더합병을 진행
                    sub_merge_folder_names = total_folder[merge_folder_name].keys()
                else:
                    continue
                for sub_merge_folder_name in sub_merge_folder_names:
                    if (
                        sub_merge_folder_name not in total_folder[folder_name].keys()
                    ):  ### 폴더안에 서브폴더 키워드가 없어야 합병을 진행함
                        merge_df = total_folder[merge_folder_name][
                            sub_merge_folder_name
                        ]
                        total_folder[folder_name][sub_merge_folder_name] = merge_df

                    # print(f'{merge_folder_name}의 {sub_merge_folder_name}을 {folder_name}에 병합 완료함')

                del total_folder[merge_folder_name]

    main_dict = {"main": {x: y for x, y in total_folder.items()}}
    total_folder.update(main_dict)
    total_folder = {
        key: value
        for key, value in total_folder.items()
        if key in ["main", "uncategorized"]
    }
    del total_folder["main"]["uncategorized"]

    ### unique data 만드는곳 ###
    main_folder_data = []
    for folder_name in total_folder["main"].keys():
        sub_folders = total_folder["main"][folder_name].keys()
        for sub_folder in sub_folders:
            df = total_folder["main"][folder_name][sub_folder]
            a = df.copy()
            a["main_folder"] = folder_name
            a["sub_folder"] = sub_folder
            main_folder_data.append(a)

    main_folder_df = pd.concat(main_folder_data)
    main_folder_df['category'] = 'main'

    #### 언카테 폴더로 만드는 데이터 ####

    uncate_data = []
    for folder_name in total_folder["uncategorized"].keys():
        sub_folders = total_folder["uncategorized"][folder_name].keys()
        for sub_folder in sub_folders:
            df = total_folder["uncategorized"][folder_name][sub_folder]
            a = df.copy()
            a["main_folder"] = folder_name
            a["sub_folder"] = sub_folder
            uncate_data.append(a)

    uncate_df = pd.concat(uncate_data)
    uncate_df['category'] = 'uncate'

    return main_folder_df, uncate_df



def create_table(name):
    conn = connect_to_db()
    cursor = conn.cursor()
    query = f"""
    CREATE TABLE "{name}" (
        search_query text,
        search_query_volume integer,
        market_impressions integer,
        brand_impressions integer,
        impressions_market_share integer,
        market_clicks integer,
        brand_clicks integer,
        "#1 clicks" integer,
        "#2 clicks" integer,
        "#3 clicks" integer,
        market_ctr integer,
        brand_ctr integer,
        ctr_gap integer,
        brand_clicks_share integer,
        market_conversions integer,
        brand_Conversions integer,
        "#1 conversion" integer,
        "#2 conversion" integer,
        "#3 conversion" integer,
        purchase_market_share integer,
        market_cvr integer,
        brand_cvr integer,
        "#1 cvr" integer,
        "#2 cvr" integer,
        "#3 cvr" integer,
        "#1 cvr gap" integer,
        "#2 cvr gap" integer,
        "#3 cvr gap" integer,
        cvr_gap integer,
        ppc_sales_7days integer,
        ppc_spend integer,
        ppc_impressions integer,
        ppc_clicks integer,
        ppc_orders_7days integer,
        ppc_ctr integer,
        ppc_cvr integer,
        ppc_acos integer,
        folder_config text[]
           );
        """
    "Search Query Tag"
    # text ARRAY
    cursor.execute(query)
    conn.commit()
    print(f"{name} 테이블 추가 완료")
    conn.close()

def insert_data(name, data):  # 모든 문자열 열에 대해 replace 수행
    conn = connect_to_db()
    cursor = conn.cursor()
    str_columns = data.select_dtypes(include=["object"])
    data[str_columns.columns] = str_columns.applymap(lambda x: x.replace("'", "''"))

    # 데이터 일괄 처리를 위한 쿼리 생성
    values = []
    for row in data.itertuples():
        values.append(
            f"('{row.search_query}', '{row.search_query_volume}', '{row.market_impressions}', '{row.brand_impressions}', {row.impressions_market_share}, '{row.market_clicks}',"
            f" '{row.brand_clicks}', '{row._7}', '{row._8}', '{row._9}', '{row.market_ctr}', '{row.brand_ctr}', '{row.ctr_gap}', '{row.brand_clicks_share}', '{row.market_conve}' )"
        )

    query = f"""INSERT INTO "{name}" (category, main_folder, sub_folder, search_query, search_volume, original_search_query, priority, kw_push, phase, difficulty_level) VALUES {', '.join(values)}"""

    try:
        cursor.execute(query)
        conn.commit()
        print(f"{name} 태그 데이터 저장 완료")
    except Exception as e:
        print(f"데이터 삽입 중 오류 발생: {e}")
        conn.rollback()
    conn.close()

def kw_normalization(data):
    data["kw_split"] = data.search_query.apply(lambda x: x.split())
    data["kw_split"] = data.kw_split.apply(
        lambda x: [
            kw
            for kw in x
            if any(char.isalpha() or char.isdigit() for char in kw)
            and kw not in stop_words  # 불용어와 기호 제거
        ]
    )

    data["kw_split_lemma"] = data.kw_split.apply(
        lambda x: [
            kw if pos_tag([kw])[0][1] == "NN" else lemmatizer.lemmatize(kw) for kw in x
        ]
    )
    data.rename(columns={"search_query": "original_search_query"}, inplace=True)
    data["search_query"] = data.kw_split_lemma.apply(lambda x: " ".join(x))

    return data

def make_folder(data, word_count, main_keywords_huddle, sub_keywords_huddle):
    print("Maker folder Start !!")
    data = data.sort_values("search_query_volume", ascending=False)

    data["kw_split"] = data.search_query.apply(lambda x: x.split())
    data.index = np.arange(len(data))

    data = kw_normalization(data)  # 써치쿼리를 nomarlization
    sq_volumes = list(zip(data.search_query, data.search_query_volume))

    total_folder = {}
    while True:
        top_folder_name = sq_volumes[0][0]
        total_sub_folder_df = data[
            data.search_query.apply(
                lambda x : top_folder_name in x
            )
        ]
        sub_folder_keywords = total_sub_folder_df.search_query.tolist()

        total_sub_folder = {}
        while True:
            sub_folder_name = sub_folder_keywords[0]
            sub_folder_name_split = sub_folder_name.split()
            sub_total_df = total_sub_folder_df[
                total_sub_folder_df.search_query.apply(
                    lambda x: sub_folder_name in x
                )
            ]

            total_sub_keywords = sub_total_df.search_query.tolist()

            total_sub_folder[sub_folder_name] = sub_total_df
            if len(sub_folder_name_split) > 1:
                sub_folder_keywords = list(
                    filter(lambda x: x not in total_sub_keywords, sub_folder_keywords)
                )
            else:
                sub_folder_keywords.remove(sub_folder_name)

            # print(
            #     f'top folder name >> {top_folder_name}, sub_folder_name >> {sub_folder_name}, sub keyword 갯수 >> {len(sub_total_df)}, sub_folder갯수 >> {len(sub_folder_keywords)}')
            if len(sub_folder_keywords) == 0:
                break

        total_folder[top_folder_name] = total_sub_folder
        sq_volumes = list(filter(lambda x: top_folder_name not in x[0], sq_volumes))

        if len(sq_volumes) == 0:
            break

    ### line 123 ~ 143 / top folder uncate 만들기 ###
    uncate_main_folders = []

    for main_folder in total_folder.keys():
        sub_folders = total_folder[main_folder].keys()
        sub_folder_dfs = []
        for sub_folder in sub_folders:
            sub_folder_df = total_folder[main_folder][sub_folder]
            sub_folder_dfs.append(sub_folder_df)

        sub_folder_dfs = pd.concat(sub_folder_dfs)
        sub_folder_dfs = sub_folder_dfs.drop_duplicates(subset="search_query")
        # print(f'써브폴더 dfs 갯수 >> {len(sub_folder_dfs)}, 메인키워드 허들 갯수 >> {main_keywords_huddle}, 타입1 >> {type(len(sub_folder_dfs))}, 타입2 >> {type(main_keywords_huddle)}')
        if (
            len(sub_folder_dfs) < main_keywords_huddle
            or len(main_folder.split()) > word_count
        ):
            uncate_main_folders.append(main_folder)

    uncate_dfs = {}
    for uncate_folder in uncate_main_folders:
        uncate_dfs[uncate_folder] = total_folder[uncate_folder]
        del total_folder[uncate_folder]

    total_folder["uncategorized"] = uncate_dfs

    ### line 145 ~ 161  / sub_folder keyword 의 수가 적으면 언카테고리로 이동시킴 ###
    for folder_name in total_folder.keys():
        if folder_name == "uncategorized":
            continue
        sub_folder_list = total_folder[folder_name].keys()
        sub_uncate_dfs = []
        remove_sub_folders = []
        for sub_folder in sub_folder_list:
            sub_folder_df = total_folder[folder_name][sub_folder]
            if len(sub_folder_df) < sub_keywords_huddle:
                remove_sub_folders.append(sub_folder)
                sub_uncate_dfs.append(sub_folder_df)
                # print(f'sub_folder >> {sub_folder}, {len(sub_folder_df)} 언카테고리로 이동')
        if len(sub_uncate_dfs) > 0:
            total_folder[folder_name]["uncategorized"] = pd.concat(sub_uncate_dfs)
            for remove_folder_name in remove_sub_folders:
                del total_folder[folder_name][remove_folder_name]

    ### line 96 ~ 121 / 폴더명이 긴 폴더를 >> 폴더명이 짧은 폴더로 합치기 위한 코드 ###
    sorted_total_folder_names = sorted(
        list(total_folder.keys()), key=len
    )  # 폴더이름이 짧은게 위로 올라오도록 정렬

    for folder_name in sorted_total_folder_names:
        if folder_name not in total_folder.keys():
            # print(folder_name, '폴더는 이미 제거됨')
            continue

        merge_folder_names = list(filter(lambda x: folder_name in x and folder_name != x, sorted_total_folder_names))

        if len(merge_folder_names) >= 1:
            for merge_folder_name in merge_folder_names:
                if (
                    merge_folder_name in total_folder.keys()
                ):  ### 토탈폴더에 지워야 될 폴더가 있으면 폴더합병을 진행
                    sub_merge_folder_names = total_folder[merge_folder_name].keys()
                else:
                    continue
                for sub_merge_folder_name in sub_merge_folder_names:
                    if (
                        sub_merge_folder_name not in total_folder[folder_name].keys()
                    ):  ### 폴더안에 서브폴더 키워드가 없어야 합병을 진행함
                        merge_df = total_folder[merge_folder_name][
                            sub_merge_folder_name
                        ]
                        total_folder[folder_name][sub_merge_folder_name] = merge_df

                    # print(f'{merge_folder_name}의 {sub_merge_folder_name}을 {folder_name}에 병합 완료함')

                del total_folder[merge_folder_name]

    main_dict = {"main": {x: y for x, y in total_folder.items()}}
    total_folder.update(main_dict)
    total_folder = {
        key: value
        for key, value in total_folder.items()
        if key in ["main", "uncategorized"]
    }
    del total_folder["main"]["uncategorized"]

    ### unique data 만드는곳 ###
    main_folder_data = []
    for folder_name in total_folder["main"].keys():
        sub_folders = total_folder["main"][folder_name].keys()
        for sub_folder in sub_folders:
            df = total_folder["main"][folder_name][sub_folder]
            a = df.copy()
            a["main_folder"] = folder_name
            a["sub_folder"] = sub_folder
            main_folder_data.append(a)

    main_folder_df = pd.concat(main_folder_data)
    main_folder_df['category'] = 'main'

    #### 언카테 폴더로 만드는 데이터 ####

    uncate_data = []
    for folder_name in total_folder["uncategorized"].keys():
        sub_folders = total_folder["uncategorized"][folder_name].keys()
        for sub_folder in sub_folders:
            df = total_folder["uncategorized"][folder_name][sub_folder]
            a = df.copy()
            a["main_folder"] = folder_name
            a["sub_folder"] = sub_folder
            uncate_data.append(a)

    uncate_df = pd.concat(uncate_data)
    uncate_df['category'] = 'uncate'

    return main_folder_df, uncate_df

if __name__ == '__main__':
    # frdate = '2024-01-01'
    # todate = '2024-12-31'
    # asin = 'B09G6BWNDP'
    # sheet_maker(frdate, todate, asin)
    create_table('zzz')
