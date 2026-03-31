@echo off
title 💎 Stock Shadow Dashboard
:: Переходим в папку проекта
cd /d "D:\Data_Analytics\GitHub\projects\autonomous-stock-shadow-etl"

:: Активируем виртуальное окружение и запускаем дашборд
echo 🚀 Запуск аналитической панели...
call venv\Scripts\python.exe -m streamlit run src/app.py

:: Если что-то пойдет не так, окно не закроется сразу
pause