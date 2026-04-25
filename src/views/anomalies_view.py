import streamlit as st
import pandas as pd
import re
import db

COL_RATIOS = [2, 4, 1, 1, 1, 2]
HEADERS_ANOMALIES = ["Артикул", "Наименование", "Было", "Стало", "Δ", "Действие"]

def show(df_anomalies, df_inv):
    active_anom = df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)] if not df_anomalies.empty else pd.DataFrame()

    if not active_anom.empty:
        # Берем только приходы (Дельта > 0)
        arrivals = active_anom[active_anom['Дельта'] > 0]
        
        if not arrivals.empty:
            with db.get_connection() as conn:
                # Загружаем ожидаемые приходы
                expected = pd.read_sql_query("SELECT * FROM expected_deliveries WHERE status = 'Ожидает'", conn)
                
                if not expected.empty:
                    for idx, anom_row in arrivals.iterrows():
                        # Ищем совпадение по имени ИЛИ артикулу И количеству
                        match = expected[
                            ((expected['item_name'] == anom_row['Наименование']) | (expected['sku'] == anom_row['Артикул'])) & 
                            (expected['qty_expected'] == anom_row['Дельта'])
                        ]
                        
                        if not match.empty:
                            match_id = match.iloc[0]['id']
                            
                            # Нашли! Сами легализуем аномалию
                            db.save_anomaly_to_db({
                                "item_name": anom_row['Наименование'],
                                "anomaly_type": "📦 Плановый приход",
                                "qty_system": anom_row['Стало'],
                                "qty_physical": anom_row['Было'], 
                                "financial_impact": 0,
                                "source": "Автоматически (Нейро-приемка)",
                                "status": "Закрыта", 
                                "comment": f"Авто-матчинг с накладной #{match_id}"
                            })
                            
                            # Помечаем в буфере, что этот товар принят
                            conn.execute("UPDATE expected_deliveries SET status = 'Принято' WHERE id = ?", (int(match_id),))
                            conn.commit()
                            
                            # Скрываем с экрана
                            st.session_state.dismissed_names.append(anom_row['Наименование'])
                            st.toast(f"🤖 Авто-приемка: {anom_row['Наименование']}")
                            st.rerun()

    if active_anom.empty: 
        st.success("Аномалий нет.")
    else:
        cols = st.columns(COL_RATIOS)
        for i, h in enumerate(HEADERS_ANOMALIES): cols[i].write(f"**{h}**")
        st.divider()
        for idx, row in active_anom.iterrows():
            with st.container():
                c = st.columns(COL_RATIOS)
                c[0].write(row['Артикул'])
                
                hist_count = row.get('history_count', 0)
                old_alias = row.get('old_name_alias', None)
                old_sku = row.get('old_sku_alias', None)
                qty_old = row['Было']

                # 1. ПРОВЕРКА НА АКТИВНЫЙ ТОВАР
                if qty_old > 0:
                    status_tag = "📦 ДОВОЗ"
                    help_text = "Обычное пополнение активного товара."
                    color = "gray"
                
                # 2. ПРОВЕРКИ НА ОБНОВЛЕНИЕ КАРТОЧКИ (Оранжевая зона)
                elif pd.notna(old_alias) and old_alias:
                    status_tag = "📝 СМЕНИЛОСЬ ИМЯ"
                    help_text = f"Артикул знаком, но раньше назывался: {old_alias}."
                    color = "orange"
                elif pd.notna(old_sku) and old_sku:
                    status_tag = "📝 СМЕНИЛСЯ АРТИКУЛ"
                    help_text = f"Имя знакомо, но старый артикул был: {old_sku}."
                    color = "orange"
                    
                # 3. ПРОВЕРКИ НА ВОЗВРАТ И НОВИНКУ
                elif hist_count > 0:
                    status_tag = "🔄 ВОЗВРАТ"
                    help_text = "Товар уже был в базе, но отсутствовал некоторое время. Жми 'Плановый приход'."
                    color = "blue"
                else:
                    status_tag = "✨ НОВИНКА"
                    help_text = "Абсолютно новый товар. В базе истории нет."
                    color = "green"

                # Название + Индикатор во второй колонке
                with c[1]:
                    st.write(row['Наименование'])
                    st.caption(f":{color}[{status_tag}] {help_text}")

                c[2].write(row['Было'])
                c[3].write(row['Стало'])
                c[4].write(f":green[+{row['Дельта']}]")

                # Умная группировка кнопок (Сетка 3x3 вместо одной длинной строки)
                # Ряд 1: Негативные инциденты (Потери и сбои)
                row1 = [("Утеря", "минус"), ("Тихая отмена", "отмена"), ("Системная ошибка", "sys_err")]
                # Ряд 2: Пересорты и излишки (Смещение остатков)
                row2 = [("Пересорт (Склад)", "склад"), ("Пересорт (1С)", "офис"), ("Излишек", "плюс")]
                # Ряд 3: Рутина и автоматизация (Системные корректировки)
                row3 = [("📦 Плановый приход", "delivery"), ("⏳ Догруз с сайта", "late_sync"), ("🔄 Обновление карточки", "card_update")]
                
                grid = [row1, row2, row3]

                # Отрисовываем сетку
                for button_row in grid:
                    btn_cols = st.columns(len(button_row))
                    for i, (label, key_suffix) in enumerate(button_row):
                        
                        # Наша кнопка вызова меню склейки
                        if label == "🔄 Обновление карточки":
                            if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                                st.session_state.link_target_idx = idx
                                st.rerun()
                        else:
                            # Логика для всех остальных обычных кнопок
                            if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                                price = df_inv[df_inv['Наименование'] == row['Наименование']]['Цена'].values[0] if not df_inv.empty else 0
                                final_status = "Закрыта" if label in ["Системная ошибка", "📦 Плановый приход", "⏳ Догруз с сайта"] else "Открыта"
                                
                                auto_comment = ""
                                if label == "📦 Плановый приход": auto_comment = "Штатное поступление товара"
                                elif label == "⏳ Догруз с сайта": auto_comment = "Запоздалая выгрузка остатков витрины"
                                
                                anomaly_data = {
                                    "item_name": row['Наименование'],
                                    "anomaly_type": label,
                                    "qty_system": row['Стало'],
                                    "qty_physical": row['Было'], 
                                    "financial_impact": abs(row['Дельта'] * price) if label not in ["Системная ошибка", "📦 Плановый приход", "⏳ Догруз с сайта"] else 0,
                                    "source": "Автоматически",
                                    "status": final_status, 
                                    "comment": auto_comment
                                }
                                db.save_anomaly_to_db(anomaly_data)
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.success(f"Зафиксировано: {label}")
                                st.rerun()

                # --- МЕНЮ СКЛЕЙКИ ИСТОРИИ (КАК НА СКЛАДЕ) ---
                if st.session_state.get('link_target_idx') == idx:
                    st.write("---")
                    
                    # Верхний ряд с кнопками пропуска и отмены (чтобы всегда были под рукой)
                    col_top1, col_top2 = st.columns([3, 1])
                    if col_top1.button("⏭️ Просто обновить карточку (БЕЗ склейки с историей)", key=f"skip_link_{idx}"):
                        db.save_anomaly_to_db({
                            "item_name": row['Наименование'],
                            "anomaly_type": "🔄 Обновление карточки",
                            "qty_system": row['Стало'],
                            "qty_physical": row['Было'], 
                            "financial_impact": 0,
                            "source": "Автоматически",
                            "status": "Закрыта", 
                            "comment": "Изменилось название на сайте"
                        })
                        st.session_state.dismissed_names.append(row['Наименование'])
                        st.session_state.link_target_idx = None
                        st.rerun()
                        
                    if col_top2.button("❌ Отмена", key=f"cancel_link_{idx}"):
                        st.session_state.link_target_idx = None
                        st.rerun()

                    st.write("") # Небольшой отступ

                    # 1. Поле ручного поиска (один в один как на складе)
                    search_query = st.text_input("🔍 Поиск старой карточки для привязки:", 
                                                placeholder="Артикул или название...",
                                                key=f"search_link_{idx}")

                    matched_df = pd.DataFrame()
                    
                    # 2. Движок поиска от вкладки Склад
                    if search_query:
                        # Убиваем мусорные значки
                        clean_query = re.sub(r'\(снят с сайта.*?\)', '', search_query, flags=re.IGNORECASE)
                        clean_query = clean_query.replace('🔘', '').replace('❌', '').strip()
                        query_words = clean_query.lower().replace('ё', 'е').split()
                        
                        if query_words:
                            mask = pd.Series(True, index=df_inv.index)
                            for word in query_words: 
                                mask &= df_inv['_search_index'].str.contains(word, regex=False)
                            
                            matched_df = df_inv[mask].copy()
                            # ПРИОРИТЕТ: Сортируем так, чтобы неактивные (снятые с сайта) были на самом верху
                            matched_df = matched_df.sort_values(by='actual', ascending=True).head(30)
                            st.caption(f"🔍 Найдено: {len(df_inv[mask])}. Показаны первые 30.")
                    else:
                        # Если ничего не введено, показываем кандидатов, пропавших с сайта
                        today_lost = df_anomalies[
                            (df_anomalies['Дельта'] < 0) & 
                            (~df_anomalies['Наименование'].isin(st.session_state.dismissed_names))
                        ]['Наименование'].tolist()
                        
                        mask1 = df_inv['Наименование'].isin(today_lost)
                        mask2 = ~df_inv['actual']
                        matched_df = df_inv[mask1 | mask2].sort_values(by='actual', ascending=True).head(10).copy()
                        st.caption("Показаны недавно пропавшие товары. Используйте поиск, чтобы найти другие.")

                    # 3. ОТРИСОВКА РЕЗУЛЬТАТОВ КАК НА СКЛАДЕ (Таблица вместо списка)
                    if not matched_df.empty:
                        hc = st.columns([2, 4, 2, 2])
                        for i, h in enumerate(["Артикул", "Наименование", "Статус", "Действие"]): 
                            hc[i].write(f"**{h}**")
                        st.divider()
                        
                        for matched_idx, m_row in matched_df.iterrows():
                            c = st.columns([2, 4, 2, 2])
                            c[0].write(m_row['Артикул'])
                            
                            display_name = m_row['Наименование']
                            if not m_row['actual']:
                                c[1].write(f"🔘 {display_name}")
                                c[2].write(f"❌ Снят ({m_row['last_seen_date']})")
                            else:
                                c[1].write(display_name)
                                c[2].write("✅ Активен")
                                
                            # Кнопка склейки прямо в строке товара!
                            if c[3].button("🔗 Склеить", key=f"do_link_{idx}_{matched_idx}", type="primary"):
                                old_name = m_row['Наименование']
                                with db.get_connection() as conn:
                                    conn.execute("INSERT INTO item_aliases (new_name, old_name) VALUES (?, ?)", (row['Наименование'], old_name))
                                    conn.execute("""
                                        INSERT INTO anomaly_log (detected_at, item_name, anomaly_type, qty_system, qty_physical, financial_impact, source, status, comment)
                                        VALUES (datetime('now', 'localtime'), ?, '🔄 Обновление карточки', 0, 0, 0, 'Автоматически', 'Закрыта', ?)
                                    """, (old_name, f"🔗 Склеено (старое имя). Новое: {row['Наименование']}"))
                                    conn.commit()
                                if old_name not in st.session_state.dismissed_names:
                                    st.session_state.dismissed_names.append(old_name)

                                db.save_anomaly_to_db({
                                    "item_name": row['Наименование'],
                                    "anomaly_type": "🔄 Обновление карточки",
                                    "qty_system": row['Стало'],
                                    "qty_physical": row['Было'], 
                                    "financial_impact": 0,
                                    "source": "Автоматически",
                                    "status": "Закрыта", 
                                    "comment": f"Склейка: {old_name}"
                                })
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.session_state.link_target_idx = None
                                st.rerun()
                            st.divider()
                    else:
                        st.info("По вашему запросу ничего не найдено.")
            st.divider()
