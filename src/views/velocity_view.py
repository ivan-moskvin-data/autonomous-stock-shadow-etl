import streamlit as st
import pandas as pd
import db


def show():
    if not st.session_state.get('selected_item_name'):
        st.info("👈 Перейдите во вкладку '📦 Склад', найдите нужный товар через поиск и нажмите '📈 График'.")
    else:
        target_name = st.session_state.selected_item_name
        target_sku = st.session_state.get('selected_item_sku', '')

        if st.button("🔙 Вернуться на склад", use_container_width=False):
            st.session_state.current_page = "📦 Склад"
            st.rerun()

        st.subheader(f"{target_name}")

        history = db.load_velocity_history(target_name, target_sku)

        if len(history) < 2:
            st.warning("Мало данных для годового графика. Нужно накопить хотя бы 2 среза базы.")
        else:
            diff = history['Остаток'].iloc[-1] - history['Остаток'].iloc[-2]
            c1, c2 = st.columns(2)
            c1.metric("Текущий остаток", f"{int(history['Остаток'].iloc[-1])} шт.")
            c2.metric("Сдвиг (к прошлой записи)", f"{int(diff)} шт.", delta=int(diff))

            st.line_chart(history['Остаток'])

            # --- НОВЫЙ БЛОК: ИСТОРИЯ ДВИЖЕНИЯ (ЛЕДЖЕР) ---
            st.write("---")
            st.subheader("📋 Журнал движений товара")

            # Вычисляем разницу между днями (сравниваем текущую строку с предыдущей)
            movements = history.copy().reset_index()
            movements['Дельта'] = movements['Остаток'].diff()

            # Оставляем только те дни, когда остаток реально менялся
            movements = movements.dropna(subset=['Дельта'])
            movements = movements[movements['Дельта'] != 0].copy()

            if movements.empty:
                st.info("Движений по данному товару не зафиксировано.")
            else:
                # 1. Продуктовая логика: классификация
                movements['Событие'] = movements['Дельта'].apply(
                    lambda x: "📦 Приход (или излишек)" if x > 0 else "🛒 Расход (или утеря)"
                )
                movements['Кол-во'] = movements['Дельта'].abs().astype(int)
                movements['Остаток'] = movements['Остаток'].astype(int)

                # 2. Дата фиксации парсером (когда скрипт увидел изменение)
                movements['Дата фиксации'] = movements['Дата'].dt.strftime('%Y-%m-%d')

                # 3. БИЗНЕС-ЛОГИКА (Сдвиг даты): изменение произошло ВЧЕРА днем или СЕГОДНЯ ночью
                # Отнимаем 1 день от даты фиксации
                movements['Фактическое время'] = (
                    movements['Дата'] - pd.Timedelta(days=1)
                ).dt.strftime('%Y-%m-%d') + " (вчера/ночь)"

                # 4. Формируем красивую таблицу для пользователя, сортируем от новых к старым
                display_df = movements[
                    ['Дата фиксации', 'Фактическое время', 'Событие', 'Кол-во', 'Остаток']
                ].sort_values(by='Дата фиксации', ascending=False)

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )