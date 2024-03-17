from datadive_tab import *
from asin_tab import *

if "upload_file" not in st.session_state.keys():
    st.session_state["upload_file"] = None

def main():
    st.set_page_config(layout="wide")
    tab1, tab2 = st.tabs(["ASIN", "DataDive"])

    with tab1:
        frdate = st.text_input(label = '시작일', value = "2023-11-01")
        todate = st.text_input(label = '종료일', value = "2023-12-31")
        asin = st.text_input(label = 'ASIN', value = "B09G6BWNDP")

        run_btn = st.button('실행버튼')

        if run_btn:
            with st.spinner('잠시만 기다려주세요'):
                if frdate == False or todate == False or asin == False:
                    st.error('입력값을 확인하세요')
                else:
                    df = sheet_maker(frdate, todate, asin)
                    try:
                        df = groupby_maker(df)
                        st.dataframe(df)
                    except:
                        st.error('Error 발생')



    with tab2:
        frdate = st.text_input(label='시작일', value="2023-11-01", key = 'frdate2')
        todate = st.text_input(label='종료일', value="2023-12-31", key = 'todate2')

        upload_file = st.file_uploader("Datadive 파일을 업로드 하세요!", type = ["xlsx", "csv"])
        upload_btn = st.button("Run")

        if upload_btn:
            with st.spinner('잠시만 기다려주세요 ...'):
                if upload_file:
                    if "xlsx" in upload_file.name:
                        upload_file = pd.read_excel(upload_file)
                    else:
                        upload_file = pd.read_csv(upload_file)

                    st.session_state["keywords"] = upload_file["SearchTerm"].unique().tolist()
                    df = sheet_maker_by_datadive(frdate, todate, st.session_state["keywords"])
                    try:
                        df = groupby_maker(df)
                        st.dataframe(df)
                    except:
                        st.error('에러발생')

                else:
                    st.error("업로드 파일을 확인하세요!")


if __name__ == '__main__':
    main()