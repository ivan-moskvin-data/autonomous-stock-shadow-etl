import streamlit as st
import db

def show():
    st.subheader("❄️ Анализ замороженного капитала (Dead Stock)")
    
    df_dead = db.load_dead_stock_analysis()
    
    if df_dead.empty: 
        st.info("📊 Нужно больше данных. Алгоритм выявления неликвидов заработает, когда накопится история изменений.")
    else:
        only_dead = df_dead[df_dead['Заморожен']].copy()
        only_dead['Потери'] = only_dead['Цена'] * only_dead['Остаток']
        total_frozen = only_dead['Потери'].sum()
        
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.metric("Заморожено (Итого)", f"{total_frozen:_.0f} ₽".replace('_', ' '))
            st.caption("Товары, лежащие без движения дольше нормы (медианы) по их категории.")
            
            # Фича для бизнеса: Экспорт отчета в CSV (читается в Excel)
            csv = only_dead.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать отчет (для Закупок)",
                data=csv,
                file_name='dead_stock_report.csv',
                mime='text/csv',
                use_container_width=True
            )
            
        with c2:
            # Бизнес-логика: Группируем потери по категориям
            if not only_dead.empty:
                st.write("**Где заморожены деньги (по категориям):**")
                # Pandas группирует данные, суммирует потери и сортирует по убыванию
                category_losses = only_dead.groupby('Категория')['Потери'].sum().sort_values(ascending=False)
                # Streamlit сам рисует красивый столбчатый график
                st.bar_chart(category_losses)

        st.write("---")
        st.write("**Детализация по товарам (Топ проблемных позиций):**")
        
        # Выводим таблицу, отсортированную от самых дорогих потерь к самым дешевым
        st.dataframe(only_dead.sort_values('Потери', ascending=False), use_container_width=True, column_config={
            "Потери": st.column_config.NumberColumn(format="%d ₽"),
            "Дней без движения": st.column_config.ProgressColumn(format="%d дн.", min_value=0, max_value=365)
        })