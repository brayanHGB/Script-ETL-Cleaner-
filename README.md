#  Script ETL Cleaner - TechSkills Analytics

Sistema ETL completo para análisis del mercado laboral tecnológico con minería de datos y exportación a Power BI.

## Descripción
**Script ETL Cleaner** es un sistema integral de procesamiento de datos que analiza el mercado laboral tecnológico mediante:

 **Extracción** de 3 fuentes de datos (empleos, inversiones, perfiles)
- **Limpieza** automática con eliminación de duplicados y outliers
-  **Transformación** con normalización y esquemas unificados
- **Minería de datos** con clustering y clasificación ML
-  **Warehouse consolidado** con 3,145 registros únicos
-  **Exportación Power BI** con 6 datasets optimizados

##  Resultados Clave

### Insights Descubiertos:
- **5 perfiles profesionales** segmentados por geografía y salario
- **Brecha salarial 9x** entre USA y LATAM
- **Modelo predictivo 71%** de precisión
- **Factores clave**: Ubicación > Experiencia > Tecnología
- **Mercado premium USA**: 10.2% volumen, máximo salario

### Métricas de Calidad:
- **90.96% calidad de datos** (EXCELENTE)
- **55 duplicados eliminados** automáticamente
- **82 outliers procesados** con técnicas estadísticas
- **100% sin duplicados** en datasets Power BI

## 🏗️ Arquitectura del Sistema

```
📁 proyecto_techskills/
├── 📁 src/                    # Código fuente principal
│   ├── etl_simple.py         # Orquestador principal ETL
│   ├── extraccion.py         # Módulo de extracción de datos
│   ├── limpieza.py           # Limpieza automática con ML
│   ├── transformacion.py     # Transformaciones y normalización
│   ├── validacion.py         # Control de calidad de datos
│   ├── data_mining.py        # Análisis ML (clustering, clasificación)
│   └── powerbi_export.py     # Exportación optimizada para BI
├── 📁 data/
│   ├── 📁 raw/              # Datos originales
│   └── 📁 processed/        # Datos procesados + TechWarehouse
├── 📁 output/               # Resultados y reportes
│   ├── 📁 powerbi/         # 6 datasets para Power BI
│   ├── reporte_calidad.txt  # Validación completa
│   └── metricas_etl.json   # Métricas de rendimiento
└── 📁 logs/                # Logs de ejecución
```

## 🚀 Instalación y Uso

### Prerrequisitos
```bash
Python 3.8+
pip install pandas numpy scikit-learn matplotlib seaborn
```

### Ejecución
```bash
# Ejecutar ETL completo
python src/etl_simple.py

# Solo minería de datos
python src/data_mining.py

# Solo exportación Power BI  
python src/powerbi_export.py
```

##  Datasets Generados

### Power BI (6 archivos optimizados):
- `main_data_powerbi.csv` - 3,145 registros completos
- `geography_powerbi.csv` - 7 países sin duplicados
- `technology_powerbi.csv` - 26 tecnologías principales  
- `clusters_powerbi.csv` - 5 perfiles ML identificados
- `kpis_powerbi.csv` - 11 métricas clave
- `time_metrics_powerbi.csv` - 2 períodos agregados

### Warehouse Principal:
- `TechWarehouse.csv` - Dataset consolidado (566KB, 3,145 registros únicos)

## Análisis de Machine Learning

### Clustering (K-Means):
- **5 clusters profesionales** identificados
- **Segmentación geográfica** automática
- **Perfiles por salario y edad** caracterizados

### Clasificación (Random Forest):
- **71% precisión** en predicción de empleos alta demanda
- **Factores importantes**: País (58.3%), Experiencia (21.8%), Tecnología (20.0%)

### Resultados:
- **2/4 técnicas exitosas** (clustering + clasificación)
- **Asociaciones/regresión**: datos insuficientes

## Power BI Dashboard

### Páginas Incluidas:
1. **Resumen Ejecutivo** - KPIs principales
2. **Análisis Geográfico** - Mapas y distribución salarial  
3. **Clustering ML** - Visualización de perfiles profesionales
4. **Tecnologías** - Análisis de demanda tecnológica
5. **Tendencias** - Evolución temporal

### Guías Incluidas:
- `PowerBI_Implementation_Guide.txt` - Guía paso a paso
- `dashboard_specification.json` - Especificación técnica

## 🔍 Tecnologías Analizadas

### Top Lenguajes:
1. **C#** (183 menciones, $23,910 promedio)
2. **R** (182 menciones, $25,851 promedio)  
3. **Scala** (178 menciones, $21,457 promedio)
4. **PHP** (177 menciones, $25,703 promedio)
5. **Python** (162 menciones, $22,874 promedio)

### Top Frameworks:
1. **Spark** (214 menciones) - Big Data
2. **Django** (210 menciones) - Python Web
3. **React** (206 menciones) - Frontend
4. **Vue** (206 menciones) - Frontend
5. **Flask** (198 menciones) - Python API

## Métricas de Rendimiento

- **Tiempo ejecución**: ~4 segundos
- **Registros procesados**: 3,145 únicos  
- **Calidad datos**: 90.96% EXCELENTE
- **Éxito minería**: 50% (2/4 técnicas)
- **Datasets BI**: 100% sin duplicados

## Cobertura Geográfica

### Países Analizados (7):
- **USA**: $91,015 promedio (mercado premium)
- **España**: $30,428 promedio (mercado europeo)
- **Chile**: $20,515 promedio
- **Colombia**: $21,282 promedio  
- **Argentina**: $16,467 promedio
- **México**: $15,080 promedio
- **Perú**: $10,710 promedio

### Ciudades: 36 ciudades identificadas

##  Características Técnicas

- **Arquitectura modular** con separación de responsabilidades
- **Logging completo** con trazabilidad total
- **Validación automática** de calidad de datos
- **Control de errores** y recuperación robusta
- **Optimización Power BI** sin duplicados
- **Documentación técnica** completa

##  Licencia

MIT License - Ver archivo `LICENSE` para detalles.

## Contribuciones

1. Fork del proyecto
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -m 'Agregar funcionalidad'`)
4. Push a rama (`git push origin feature/nueva-funcionalidad`)
5. Abrir Pull Request

##  Contacto

- **Autor**: brayanHGB
- **Repositorio**: https://github.com/brayanHGB/Script-ETL-Cleaner-
- **Documentación**: Ver carpeta `/output/` para guías completas

---

**🎯 Estado**: ✅ SISTEMA LISTO PARA ANÁLISIS EMPRESARIAL

*Sistema ETL TechSkills v2.0 - Noviembre 2024*
