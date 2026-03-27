import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st
import sqlite3

print("--- ИНСПЕКЦИЯ СИСТЕМЫ ---")
print(f"Pandas версия: {pd.__version__} - OK")
print(f"Requests: OK")
print(f"BeautifulSoup: OK")
print(f"Streamlit: OK")
print(f"SQLite3: OK")
print("--------------------------")
print("🚀 Все системы в норме! Мы готовы строить базу данных.")