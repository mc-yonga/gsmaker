from superDev import *

def main():
    st.set_page_config(layout="wide")

    frdate = st.text_input(label = '시작일')
    todate = st.text_input(label = '종료일')
    asin = st.text_input(label = 'ASIN')

    run_btn = st.button('실행버튼')

    if run_btn:
        with st.spinner('잠시만 기다려주세요'):
            if frdate == False or todate == False or asin == False:
                st.error('입력값을 확인하세요')
            else:
                print(frdate, todate, asin)
                df = sheet_maker(frdate, todate, asin)
                st.dataframe(df)

                groupby_result = groupby_maker(df)
                groupby_result.index.name = 'index'
                st.dataframe(groupby_result)


if __name__ == '__main__':
    main()