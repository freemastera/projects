@echo off
python D:\etl\metrika_to_postgres_backfilling_historical_data.py %* # путь к скрипту для загрузки исторических данных
pause # для отладки, если будут ошибки