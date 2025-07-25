@echo on
cd /d %~dp0
echo Aktuelles Verzeichnis: %cd%
call dataspot\Scripts\activate.bat
echo Virtuelle Umgebung wurde aktiviert
streamlit run src/sarah/streamlit/app.py
echo Streamlit wurde ausgeführt
pause