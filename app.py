from datadive_tab import *
from asin_tab import *


def main():
    st.set_page_config(layout="wide")
    tab1, tab2 = st.tabs(["ASIN", "DataDive"])

    with tab1:
        frdate = st.text_input(label = '시작일', value = "2023-11-01")
        todate = st.text_input(label = '종료일', value = "2023-12-31")
        asin = st.text_input(label = 'ASIN', value = "B09G6BWNDP")

        col1, col2, col3, cols4 = st.columns([1,1,1,5])
        with col1:
            run_btn = st.button('Run')
        with col2:
            table_name = st.text_input('저장명을 입력하세요')
        with col3:
            save_btn = st.button('Save')

        if run_btn:
            with st.spinner('잠시만 기다려주세요'):
                if frdate == False or todate == False or asin == False:
                    st.error('입력값을 확인하세요')
                else:
                    sheet_maker(frdate, todate, asin)
                    if st.session_state["asin_sheet_maker"] is not None:
                        st.session_state["asin_result_df"] = groupby_maker(st.session_state["asin_sheet_maker"])

        if save_btn:
            if table_name:
                with st.spinner('데이터를 저장 중입니다...'):

                    if st.session_state["asin_result_df"] is None:
                        st.error("데이터가 만들어지지 않았습니다. 입력일과 종료일 그리고 ASIN을 정확하게 입력했는지 확인해보세요")
                    else:
                        create_table(table_name)
                        insert_data(table_name, st.session_state["asin_result_df"])
            else:
                st.error('테이블 이름을 입력하세요')

        if st.session_state["asin_sheet_maker"] is not None:
            st.dataframe(st.session_state["asin_result_df"])


    with tab2:
        frdate = st.text_input(label='시작일', value="2023-11-01", key = 'frdate2')
        todate = st.text_input(label='종료일', value="2023-12-31", key = 'todate2')

        upload_file = st.file_uploader("Datadive 파일을 업로드 하세요!", type = ["xlsx", "csv"])

        col1, col2, col3, cols4 = st.columns([1,1,1,5])
        with col1:
            upload_btn = st.button("Run", key='dive_run')
        with col2:
            table_name = st.text_input('저장명을 입력하세요', key = 'dive_table_name')
        with col3:
            save_btn = st.button('Save', key = 'dive_save')

        if upload_btn:
            with st.spinner('잠시만 기다려주세요 ...'):
                if upload_file:
                    if "xlsx" in upload_file.name:
                        upload_file = pd.read_excel(upload_file)
                    else:
                        upload_file = pd.read_csv(upload_file)

                    st.session_state["keywords"] = upload_file["SearchTerm"].unique().tolist()
                    sheet_maker_by_datadive(frdate, todate, st.session_state["keywords"])
                    if st.session_state["dive_sheet_maker"] is not None:
                        st.session_state["dive_result_df"] = groupby_maker(st.session_state["dive_sheet_maker"])

        if save_btn:
            if table_name:
                with st.spinner('데이터를 저장 중입니다...'):
                    if st.session_state["dive_result_df"] is None:
                        st.error("데이터가 만들어지지 않았습니다. 입력일과 종료일 그리고 데이터가 제대로 업로드 되었는지 확인해보세요")
                    else:
                        create_table(table_name)
                        insert_data(table_name, st.session_state["dive_result_df"])
            else:
                st.error('테이블 이름을 입력하세요')

        if st.session_state["dive_sheet_maker"] is not None:
            st.dataframe(st.session_state["dive_result_df"])


if __name__ == '__main__':
    if "upload_file" not in st.session_state.keys():
        st.session_state["upload_file"] = None
    if "asin_sheet_maker" not in st.session_state.keys():
        st.session_state["asin_sheet_maker"] = None
    if "dive_sheet_maker" not in st.session_state.keys():
        st.session_state["dive_sheet_maker"] = None
    if "asin_result_df" not in st.session_state.keys():
        st.session_state["asin_result_df"] = None
    if "dive_result_df" not in st.session_state.keys():
        st.session_state["dive_result_df"] = None

    main()