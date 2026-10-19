import io
from pathlib import Path

import pandas as pd
import streamlit as st

from app.database import connect, results
from app.upload import read_csv, send_rows


st.set_page_config(page_title="Скоринг транзакций")
st.title("Скоринг транзакций")
upload_tab, results_tab = st.tabs(["Отправка транзакций", "Результаты"])

with upload_tab:
    st.download_button(
        "Скачать пример CSV",
        Path("examples/test.csv").read_bytes(),
        file_name="test.csv",
        mime="text/csv",
    )
    uploaded = st.file_uploader("Загрузите test.csv", type="csv")
    if uploaded and st.button("Отправить в Kafka"):
        try:
            rows = read_csv(io.StringIO(uploaded.getvalue().decode("utf-8-sig")))
            ids = send_rows(rows)
            st.success(f"Отправлено транзакций: {len(ids)}. Результаты появятся после обработки.")
        except Exception as error:
            st.error(str(error))

with results_tab:
    if st.button("Посмотреть результаты"):
        try:
            with connect() as connection:
                fraud, latest = results(connection)
            st.subheader("10 последних фродовых транзакций")
            if fraud:
                st.dataframe(pd.DataFrame(fraud), hide_index=True)
            else:
                st.info("Фродовых транзакций пока нет")
            st.subheader("Распределение скоров последних 100 транзакций")
            if latest:
                bins = [0] * 10
                for row in latest:
                    bins[min(int(row["score"] * 10), 9)] += 1
                chart = pd.DataFrame({
                    "Интервал скора": [f"{i / 10:.1f}–{(i + 1) / 10:.1f}" for i in range(10)],
                    "Транзакций": bins,
                }).set_index("Интервал скора")
                st.bar_chart(chart, x_label="Скор", y_label="Количество")
                st.caption(f"Учтено транзакций: {len(latest)}. Последний интервал включает 1.")
            else:
                st.info("Результатов пока нет")
        except Exception as error:
            st.error(str(error))
