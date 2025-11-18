@echo off
echo ==========================================
echo    PROCESO ETL - TECH SKILLS (Simple)
echo ==========================================
echo.

cd /d "c:\Users\Administrator\Desktop\proceso ETL\proyecto_techskills\src"

echo Ejecutando proceso ETL simplificado...
echo.
C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe etl_simple.py

echo.
echo ==========================================
echo PROCESO ETL COMPLETADO
echo ==========================================
echo.
echo Archivos generados:
echo [*] data/processed/ - Datos procesados
echo [*] output/ - Reportes de calidad
echo [*] logs/ - Registros de ejecucion
echo.
echo Presiona cualquier tecla para continuar...
pause > nul
