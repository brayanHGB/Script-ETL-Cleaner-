@echo off
echo ==========================================
echo    PROCESO ETL - TECH SKILLS
echo ==========================================
echo.

cd /d "c:\Users\Administrator\Desktop\proceso ETL\proyecto_techskills\src"

echo Iniciando proceso ETL...
echo.
C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe etl_main.py

echo.
echo ==========================================
echo PROCESO ETL COMPLETADO
echo ==========================================
echo.
echo Revisa los resultados en:
echo - data/processed/ (archivos de datos procesados)
echo - output/ (reportes de calidad y metricas)
echo - logs/ (registros de ejecucion)
echo.
echo Presiona cualquier tecla para continuar...
pause > nul
