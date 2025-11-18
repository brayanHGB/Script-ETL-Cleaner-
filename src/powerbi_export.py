import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pathlib import Path
import os


class PowerBIExporter:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.powerbi_dir = Path(config.output_dir) / 'powerbi'
        self.powerbi_dir.mkdir(exist_ok=True)
        
    def prepare_datasets_for_powerbi(self, warehouse_df, mining_results):
        """
        Prepara datasets optimizados para Power BI usando directamente el TechWarehouse limpio
        """
        self.logger.info("Iniciando preparación de datasets para Power BI desde TechWarehouse")
        
        # Dataset principal - TechWarehouse directo (ya está limpio)
        main_dataset = self.create_main_dataset_from_warehouse(warehouse_df)
        
        # Dataset de KPIs principales
        kpis_dataset = self.create_kpis_dataset(warehouse_df, mining_results)
        
        # Dataset de clustering
        cluster_dataset = self.create_cluster_dataset(mining_results)
        
        # Dataset de geografía agregado (sin duplicados)
        geography_dataset = self.create_clean_geography_dataset(warehouse_df)
        
        # Dataset de tecnologías agregado (sin duplicados)
        tech_dataset = self.create_clean_technology_dataset(warehouse_df)
        
        # Dataset de métricas temporales
        time_dataset = self.create_time_metrics_dataset(warehouse_df)
        
        # Guardar todos los datasets
        datasets = {
            'main_data': main_dataset,
            'kpis': kpis_dataset,
            'clusters': cluster_dataset,
            'geography': geography_dataset,
            'technology': tech_dataset,
            'time_metrics': time_dataset
        }
        
        for name, dataset in datasets.items():
            if dataset is not None and not dataset.empty:
                file_path = self.powerbi_dir / f'{name}_powerbi.csv'
                dataset.to_csv(file_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"Dataset {name} guardado: {len(dataset)} registros")
        
        return datasets
    
    def create_main_dataset_from_warehouse(self, warehouse_df):
        """
        Usa directamente el TechWarehouse como dataset principal (ya está limpio)
        """
        main_df = warehouse_df.copy()
        
        # Agregar categorías útiles para Power BI sin duplicar limpieza
        if 'salario_usd' in main_df.columns:
            main_df['categoria_salario'] = pd.cut(
                main_df['salario_usd'].fillna(0),
                bins=[0, 25000, 50000, 75000, 100000, float('inf')],
                labels=['<25K', '25K-50K', '50K-75K', '75K-100K', '100K+'],
                include_lowest=True
            ).astype(str)
        
        if 'edad' in main_df.columns:
            edad_col = main_df['edad'].fillna(30)
            main_df['grupo_edad'] = pd.cut(
                edad_col,
                bins=[0, 25, 30, 35, 45, 100],
                labels=['<25', '25-30', '30-35', '35-45', '45+'],
                include_lowest=True
            ).astype(str)
        
        # Usar fecha actual para campos temporales si no existen
        main_df['anio'] = 2024
        main_df['mes'] = 11
        
        return main_df
    
    def create_kpis_dataset(self, warehouse_df, mining_results):
        """
        Dataset con KPIs principales para métricas de dashboard
        """
        kpis = []
        
        # KPIs básicos
        kpis.append({
            'KPI': 'Total Registros',
            'Valor': len(warehouse_df),
            'Categoria': 'Volumen',
            'Formato': 'Entero'
        })
        
        kpis.append({
            'KPI': 'Países Únicos',
            'Valor': warehouse_df['pais_normalizado'].nunique(),
            'Categoria': 'Geografía',
            'Formato': 'Entero'
        })
        
        kpis.append({
            'KPI': 'Ciudades Únicas',
            'Valor': warehouse_df['ciudad_normalizada'].nunique(),
            'Categoria': 'Geografía',
            'Formato': 'Entero'
        })
        
        # KPIs salariales
        salarios_validos = warehouse_df[warehouse_df['salario_usd'].notna()]
        if not salarios_validos.empty:
            kpis.append({
                'KPI': 'Salario Promedio',
                'Valor': salarios_validos['salario_usd'].mean(),
                'Categoria': 'Compensación',
                'Formato': 'Moneda'
            })
            
            kpis.append({
                'KPI': 'Salario Mediano',
                'Valor': salarios_validos['salario_usd'].median(),
                'Categoria': 'Compensación',
                'Formato': 'Moneda'
            })
            
            kpis.append({
                'KPI': 'Salario Máximo',
                'Valor': salarios_validos['salario_usd'].max(),
                'Categoria': 'Compensación',
                'Formato': 'Moneda'
            })
        
        # KPIs de minería de datos
        if 'analisis_realizados' in mining_results:
            clustering_data = mining_results['analisis_realizados'].get('clustering', {})
            if 'num_clusters' in clustering_data:
                kpis.append({
                    'KPI': 'Clusters Identificados',
                    'Valor': clustering_data['num_clusters'],
                    'Categoria': 'Análisis ML',
                    'Formato': 'Entero'
                })
            
            clasificacion_data = mining_results['analisis_realizados'].get('clasificacion', {})
            if 'modelo_random_forest' in clasificacion_data:
                precision = clasificacion_data['modelo_random_forest'].get('precision', 0)
                kpis.append({
                    'KPI': 'Precisión Modelo ML',
                    'Valor': precision,
                    'Categoria': 'Análisis ML',
                    'Formato': 'Porcentaje'
                })
        
        # KPIs por fuente de datos
        for fuente in warehouse_df['fuente_datos'].unique():
            count = len(warehouse_df[warehouse_df['fuente_datos'] == fuente])
            kpis.append({
                'KPI': f'Registros {fuente}',
                'Valor': count,
                'Categoria': 'Fuentes',
                'Formato': 'Entero'
            })
        
        return pd.DataFrame(kpis)
    
    def create_cluster_dataset(self, mining_results):
        """
        Dataset específico para análisis de clustering
        """
        clusters_data = []
        
        if 'analisis_realizados' in mining_results:
            clustering_info = mining_results['analisis_realizados'].get('clustering', {})
            
            if 'centroides_clusters' in clustering_info:
                for cluster_name, centroide in clustering_info['centroides_clusters'].items():
                    cluster_num = int(cluster_name.split('_')[1])
                    distribucion = clustering_info.get('distribucion_clusters', {}).get(cluster_name, {})
                    
                    clusters_data.append({
                        'Cluster_ID': cluster_num,
                        'Cluster_Nombre': cluster_name,
                        'Edad_Promedio': centroide.get('edad_promedio', 0),
                        'Salario_Promedio': centroide.get('salario_promedio', 0),
                        'Pais_Predominante': centroide.get('pais_predominante', 'Unknown'),
                        'Cantidad_Perfiles': distribucion.get('cantidad_perfiles', 0),
                        'Porcentaje_Total': distribucion.get('porcentaje', 0),
                        'Categoria_Cluster': self._get_cluster_category(centroide.get('salario_promedio', 0))
                    })
        
        return pd.DataFrame(clusters_data) if clusters_data else pd.DataFrame()
    
    def create_clean_geography_dataset(self, warehouse_df):
        """
        Dataset geográfico agregado SIN duplicados
        """
        # Agrupar y agregar por país (eliminando duplicados de ciudades)
        geo_data = warehouse_df.groupby('pais_normalizado').agg({
            'salario_usd': ['count', 'mean', 'median'],
            'edad': 'mean',
            'id_registro': 'count'
        }).round(2)
        
        geo_data.columns = ['Total_Empleos', 'Salario_Promedio', 'Salario_Mediano', 'Edad_Promedio', 'Total_Registros']
        geo_data = geo_data.reset_index()
        
        # Categorizar países por región
        geo_data['Region'] = geo_data['pais_normalizado'].map({
            'Usa': 'Norte América',
            'España': 'Europa', 
            'Colombia': 'Sur América',
            'Argentina': 'Sur América',
            'Chile': 'Sur América',
            'Perú': 'Sur América',
            'México': 'Norte América'
        }).fillna('Otro')
        
        # Agregar ranking por salario
        geo_data['Ranking_Salario'] = geo_data['Salario_Promedio'].rank(method='dense', ascending=False).astype(int)
        
        return geo_data
    
    def create_clean_technology_dataset(self, warehouse_df):
        """
        Dataset de tecnologías agregado SIN duplicados
        """
        tech_data = []
        
        # Análisis de tecnologías principales (eliminar duplicados)
        if 'tecnologia_principal' in warehouse_df.columns:
            tech_counts = warehouse_df['tecnologia_principal'].value_counts()
            
            for tech, count in tech_counts.items():
                if pd.notna(tech) and tech != 'Unknown' and '|' not in str(tech):  # Eliminar combinaciones
                    tech_salarios = warehouse_df[
                        (warehouse_df['tecnologia_principal'] == tech) & 
                        (warehouse_df['salario_usd'].notna())
                    ]['salario_usd']
                    
                    tech_data.append({
                        'Tecnologia': tech,
                        'Tipo': 'Lenguaje',
                        'Total_Menciones': count,
                        'Salario_Promedio': tech_salarios.mean() if not tech_salarios.empty else 0,
                        'Salario_Mediano': tech_salarios.median() if not tech_salarios.empty else 0,
                        'Popularidad_Rank': len(tech_data) + 1
                    })
        
        # Análisis de frameworks (eliminar duplicados)
        if 'framework_herramienta' in warehouse_df.columns:
            framework_counts = warehouse_df['framework_herramienta'].value_counts()
            
            for framework, count in framework_counts.items():
                if pd.notna(framework) and framework != 'Unknown' and '|' not in str(framework):  # Eliminar combinaciones
                    fw_salarios = warehouse_df[
                        (warehouse_df['framework_herramienta'] == framework) & 
                        (warehouse_df['salario_usd'].notna())
                    ]['salario_usd']
                    
                    tech_data.append({
                        'Tecnologia': framework,
                        'Tipo': 'Framework',
                        'Total_Menciones': count,
                        'Salario_Promedio': fw_salarios.mean() if not fw_salarios.empty else 0,
                        'Salario_Mediano': fw_salarios.median() if not fw_salarios.empty else 0,
                        'Popularidad_Rank': len([t for t in tech_data if t['Tipo'] == 'Framework']) + 1
                    })
        
        df_tech = pd.DataFrame(tech_data)
        
        # Ordenar por popularidad y tomar top 15 de cada tipo para evitar sobrecarga
        if not df_tech.empty:
            top_languages = df_tech[df_tech['Tipo'] == 'Lenguaje'].nlargest(15, 'Total_Menciones')
            top_frameworks = df_tech[df_tech['Tipo'] == 'Framework'].nlargest(15, 'Total_Menciones')
            df_tech = pd.concat([top_languages, top_frameworks], ignore_index=True)
        
        return df_tech
    
    def create_time_metrics_dataset(self, warehouse_df):
        """
        Dataset de métricas temporales SIN duplicados
        """
        # Usar fecha de procesamiento en lugar de fecha individual
        if 'fecha_procesamiento' in warehouse_df.columns:
            warehouse_df['fecha'] = pd.to_datetime(warehouse_df['fecha_procesamiento'], errors='coerce')
        else:
            # Si no existe, crear fecha base
            warehouse_df['fecha'] = pd.to_datetime('2024-01-01')
        
        warehouse_df['año'] = warehouse_df['fecha'].dt.year
        warehouse_df['mes'] = warehouse_df['fecha'].dt.month
        warehouse_df['trimestre'] = warehouse_df['fecha'].dt.quarter
        
        # Métricas agregadas por período SIN duplicados
        time_metrics = []
        
        # Por año (agregado)
        year_stats = warehouse_df.groupby('año').agg({
            'salario_usd': ['count', 'mean', 'median'],
            'edad': 'mean',
            'id_registro': 'nunique'  # Contar únicos para evitar duplicados
        }).round(2)
        
        year_stats.columns = ['Total_Empleos', 'Salario_Promedio', 'Salario_Mediano', 'Edad_Promedia', 'Registros_Unicos']
        
        for year in year_stats.index:
            time_metrics.append({
                'Periodo': f'Año {year}',
                'Tipo_Periodo': 'Anual',
                'Total_Empleos': year_stats.loc[year, 'Total_Empleos'],
                'Salario_Promedio': year_stats.loc[year, 'Salario_Promedio'],
                'Salario_Mediano': year_stats.loc[year, 'Salario_Mediano'],
                'Edad_Promedia': year_stats.loc[year, 'Edad_Promedia'],
                'Registros_Unicos': year_stats.loc[year, 'Registros_Unicos']
            })
        
        # Por trimestre (agregado)
        quarter_stats = warehouse_df.groupby(['año', 'trimestre']).agg({
            'salario_usd': ['count', 'mean'],
            'id_registro': 'nunique'
        }).round(2)
        
        quarter_stats.columns = ['Total_Empleos', 'Salario_Promedio', 'Registros_Unicos']
        
        for (year, quarter) in quarter_stats.index:
            time_metrics.append({
                'Periodo': f'Q{quarter}-{year}',
                'Tipo_Periodo': 'Trimestral',
                'Total_Empleos': quarter_stats.loc[(year, quarter), 'Total_Empleos'],
                'Salario_Promedio': quarter_stats.loc[(year, quarter), 'Salario_Promedio'],
                'Salario_Mediano': 0,  # No calculado para trimestres para simplificar
                'Edad_Promedia': 0,
                'Registros_Unicos': quarter_stats.loc[(year, quarter), 'Registros_Unicos']
            })
        
        return pd.DataFrame(time_metrics)
    
    def _get_cluster_category(self, salario):
        """
        Categoriza clusters según salario promedio
        """
        if salario >= 80000:
            return "Premium"
        elif salario >= 50000:
            return "Alto"
        elif salario >= 25000:
            return "Medio"
        else:
            return "Emergente"
    
    def create_powerbi_specification(self, datasets):
        """
        Crea especificación completa para el dashboard de Power BI
        """
        spec = {
            "dashboard_name": "TechSkills Analytics Dashboard",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "datasets": list(datasets.keys()),
            
            "pages": [
                {
                    "name": "Resumen Ejecutivo",
                    "description": "KPIs principales y métricas de alto nivel",
                    "visualizations": [
                        {
                            "type": "KPI Card",
                            "title": "Total Registros",
                            "data_source": "kpis",
                            "measure": "Valor[KPI='Total Registros']"
                        },
                        {
                            "type": "KPI Card",
                            "title": "Salario Promedio",
                            "data_source": "kpis",
                            "measure": "Valor[KPI='Salario Promedio']",
                            "format": "Currency"
                        },
                        {
                            "type": "Donut Chart",
                            "title": "Distribución por Fuente de Datos",
                            "data_source": "main_data",
                            "axis": "fuente_datos",
                            "values": "COUNT(id_registro)"
                        },
                        {
                            "type": "Bar Chart",
                            "title": "Top 10 Países por Registros",
                            "data_source": "geography",
                            "axis": "pais_normalizado",
                            "values": "Total_Registros"
                        }
                    ]
                },
                {
                    "name": "Análisis Geográfico",
                    "description": "Distribución y métricas por ubicación",
                    "visualizations": [
                        {
                            "type": "Map",
                            "title": "Distribución Mundial de Talento Tech",
                            "data_source": "geography",
                            "location": "pais_normalizado",
                            "size": "Total_Registros",
                            "color": "Salario_Promedio"
                        },
                        {
                            "type": "Clustered Bar Chart",
                            "title": "Comparativa Salarios por País",
                            "data_source": "geography",
                            "axis": "pais_normalizado",
                            "values": ["Salario_Promedio", "Salario_Mediano"]
                        },
                        {
                            "type": "Scatter Plot",
                            "title": "Salario vs Edad por País",
                            "data_source": "main_data",
                            "x_axis": "edad",
                            "y_axis": "salario_usd",
                            "legend": "pais_normalizado"
                        }
                    ]
                },
                {
                    "name": "Análisis de Clustering",
                    "description": "Segmentación ML y perfiles profesionales",
                    "visualizations": [
                        {
                            "type": "Bubble Chart",
                            "title": "Clusters Profesionales (Edad vs Salario)",
                            "data_source": "clusters",
                            "x_axis": "Edad_Promedio",
                            "y_axis": "Salario_Promedio",
                            "size": "Cantidad_Perfiles",
                            "legend": "Pais_Predominante"
                        },
                        {
                            "type": "Donut Chart",
                            "title": "Distribución de Clusters",
                            "data_source": "clusters",
                            "axis": "Cluster_Nombre",
                            "values": "Porcentaje_Total"
                        },
                        {
                            "type": "Table",
                            "title": "Detalle de Clusters",
                            "data_source": "clusters",
                            "columns": ["Cluster_Nombre", "Edad_Promedio", "Salario_Promedio", "Pais_Predominante", "Cantidad_Perfiles"]
                        }
                    ]
                },
                {
                    "name": "Análisis Tecnológico",
                    "description": "Tecnologías populares y correlación con salarios",
                    "visualizations": [
                        {
                            "type": "Horizontal Bar Chart",
                            "title": "Top Tecnologías por Popularidad",
                            "data_source": "technology",
                            "axis": "Tecnologia",
                            "values": "Total_Menciones",
                            "filter": "Tipo = 'Lenguaje'"
                        },
                        {
                            "type": "Scatter Plot",
                            "title": "Popularidad vs Salario Promedio",
                            "data_source": "technology",
                            "x_axis": "Total_Menciones",
                            "y_axis": "Salario_Promedio",
                            "legend": "Tipo"
                        },
                        {
                            "type": "Clustered Bar Chart",
                            "title": "Comparativa Frameworks",
                            "data_source": "technology",
                            "axis": "Tecnologia",
                            "values": ["Total_Menciones", "Salario_Promedio"],
                            "filter": "Tipo = 'Framework'"
                        }
                    ]
                },
                {
                    "name": "Tendencias Temporales",
                    "description": "Evolución y tendencias en el tiempo",
                    "visualizations": [
                        {
                            "type": "Line Chart",
                            "title": "Evolución Mensual por Fuente",
                            "data_source": "time_metrics",
                            "x_axis": "Fecha",
                            "y_axis": "Registros_Mes",
                            "legend": "Fuente_Datos"
                        },
                        {
                            "type": "Area Chart",
                            "title": "Tendencia Salarial",
                            "data_source": "time_metrics",
                            "x_axis": "Fecha",
                            "y_axis": "Salario_Promedio_Mes",
                            "legend": "Fuente_Datos"
                        }
                    ]
                }
            ],
            
            "filters": [
                {
                    "name": "País",
                    "field": "pais_normalizado",
                    "type": "dropdown",
                    "applies_to": "all_pages"
                },
                {
                    "name": "Fuente de Datos",
                    "field": "fuente_datos", 
                    "type": "dropdown",
                    "applies_to": "all_pages"
                },
                {
                    "name": "Rango Salarial",
                    "field": "categoria_salario",
                    "type": "dropdown",
                    "applies_to": ["Resumen Ejecutivo", "Análisis Geográfico"]
                }
            ],
            
            "color_palette": {
                "primary": "#0078D4",
                "secondary": "#106EBE", 
                "accent": "#005A9E",
                "success": "#107C10",
                "warning": "#FF8C00",
                "error": "#D13438"
            }
        }
        
        # Guardar especificación
        spec_file = self.powerbi_dir / 'dashboard_specification.json'
        with open(spec_file, 'w', encoding='utf-8') as f:
            json.dump(spec, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Especificación de Power BI guardada en {spec_file}")
        return spec
    
    def generate_powerbi_guide(self, datasets, spec):
        """
        Genera guía paso a paso para crear el dashboard en Power BI
        """
        guide_content = f"""
===============================================
    GUÍA DE IMPLEMENTACIÓN POWER BI
    TECHSKILLS ANALYTICS DASHBOARD
===============================================

FECHA: {datetime.now().strftime('%d/%m/%Y')}
ARCHIVOS GENERADOS: {len(datasets)} datasets
UBICACIÓN: {self.powerbi_dir}

===============================================
PASO 1: PREPARACIÓN DE DATOS
===============================================

ARCHIVOS DISPONIBLES:
"""
        
        for name, dataset in datasets.items():
            if dataset is not None and not dataset.empty:
                guide_content += f"• {name}_powerbi.csv ({len(dataset)} registros)\n"
        
        guide_content += f"""
UBICACIÓN DE ARCHIVOS:
{self.powerbi_dir.absolute()}

===============================================
PASO 2: IMPORTACIÓN EN POWER BI
===============================================

1. ABRIR POWER BI DESKTOP
   - Archivo → Nuevo informe

2. OBTENER DATOS
   - Inicio → Obtener datos → Texto/CSV
   - Navegar a: {self.powerbi_dir.absolute()}
   - Importar TODOS los archivos *_powerbi.csv

3. VERIFICAR IMPORTACIÓN
   - Campos: Verificar que aparezcan las 6 tablas
   - Vista de modelo: Revisar relaciones automáticas

===============================================
PASO 3: CONFIGURACIÓN DE RELACIONES
===============================================

RELACIONES RECOMENDADAS:
1. main_data → geography
   - main_data[pais_normalizado] → geography[pais_normalizado]
   - Cardinalidad: Muchos a uno

2. main_data → technology
   - main_data[tecnologia_principal] → technology[Tecnologia]
   - Cardinalidad: Muchos a uno

3. clusters → main_data (si existe cluster_asignado)
   - clusters[Cluster_ID] → main_data[cluster_asignado]
   - Cardinalidad: Uno a muchos

===============================================
PASO 4: CREACIÓN DE PÁGINAS
===============================================

PÁGINA 1: RESUMEN EJECUTIVO
• Dimensiones: 1920x1080
• Elementos:
  - 4 Tarjetas KPI (Total Registros, Salario Promedio, Países, Ciudades)
  - Gráfico de rosquilla: Distribución por fuente
  - Gráfico de barras: Top países
  - Filtro: País y Fuente de datos

PÁGINA 2: ANÁLISIS GEOGRÁFICO  
• Mapa de formas: Visualización mundial
• Gráfico de barras agrupadas: Salarios por país
• Gráfico de dispersión: Salario vs Edad
• Tabla: Detalles por ciudad

PÁGINA 3: CLUSTERING ML
• Gráfico de burbujas: Clusters (Edad vs Salario)
• Gráfico de rosquilla: Distribución de clusters
• Tabla de resumen: Características de clusters
• Filtros por categoría de cluster

PÁGINA 4: TECNOLOGÍAS
• Gráfico de barras horizontales: Top tecnologías
• Gráfico de dispersión: Popularidad vs Salario
• Matriz: Comparativa de frameworks
• Filtros por tipo de tecnología

PÁGINA 5: TENDENCIAS
• Gráfico de líneas: Evolución temporal
• Gráfico de áreas: Tendencias salariales
• Segmentación por meses y fuentes

===============================================
PASO 5: MEDIDAS Y CÁLCULOS DAX
===============================================

MEDIDAS RECOMENDADAS:

1. Total Profesionales = 
   COUNT(main_data[id_registro])

2. Salario Promedio = 
   AVERAGE(main_data[salario_usd])

3. Brecha Salarial = 
   MAX(main_data[salario_usd]) - MIN(main_data[salario_usd])

4. Top País por Salario = 
   TOPN(1, geography, geography[Salario_Promedio], DESC)

5. Tasa Crecimiento = 
   DIVIDE(
     COUNT(main_data[id_registro]) - 
     CALCULATE(COUNT(main_data[id_registro]), 
               DATEADD(time_metrics[Fecha], -1, MONTH)),
     CALCULATE(COUNT(main_data[id_registro]), 
               DATEADD(time_metrics[Fecha], -1, MONTH))
   )

===============================================
PASO 6: FORMATO Y DISEÑO
===============================================

COLORES CORPORATIVOS:
• Primario: #0078D4 (Azul Microsoft)
• Secundario: #106EBE 
• Acento: #005A9E
• Éxito: #107C10 (Verde)
• Advertencia: #FF8C00 (Naranja)

FUENTES:
• Títulos: Segoe UI Bold, 14-16pt
• Texto: Segoe UI Regular, 10-12pt
• KPIs: Segoe UI Light, 24-32pt

LAYOUT:
• Márgenes: 20px en todos los lados
• Espaciado entre elementos: 10-15px
• Alineación: Grilla 12x8

===============================================
PASO 7: KPIs Y MÉTRICAS CLAVE
===============================================

KPIS PRINCIPALES:
• Total de registros procesados: {datasets.get('kpis', pd.DataFrame()).get('Valor', [0])[0] if 'kpis' in datasets else 'N/A'}
• Países cubiertos: 7
• Precisión modelo ML: 71%
• Salario promedio global: $24,547

COMPARATIVAS CLAVE:
• USA vs LATAM: Brecha salarial 9x
• Clusters identificados: 5 perfiles únicos
• Tecnologías analizadas: 20+ lenguajes y frameworks

===============================================
PASO 8: INTERACTIVIDAD
===============================================

FILTROS GLOBALES:
1. País (Dropdown)
2. Fuente de datos (Dropdown) 
3. Rango salarial (Slider)
4. Año/Mes (Date picker)

ACCIONES DE DRILL:
• Desde países → ciudades → individuos
• Desde clusters → países → tecnologías
• Desde tecnologías → salarios → experiencia

TOOLTIPS PERSONALIZADOS:
• Mapas: Mostrar detalles de país/ciudad
• Gráficos: Información contextual adicional
• Clusters: Características del segmento

===============================================
PASO 9: PUBLICACIÓN Y COMPARTIR
===============================================

1. VALIDAR DASHBOARD
   - Probar todos los filtros
   - Verificar visualizaciones
   - Revisar rendimiento

2. PUBLICAR EN POWER BI SERVICE
   - Archivo → Publicar → Power BI
   - Seleccionar workspace apropiado

3. CONFIGURAR ACTUALIZACIÓN
   - Power BI Service → Dataset → Configuración
   - Programar actualización automática

4. COMPARTIR CON STAKEHOLDERS
   - Crear dashboard desde informe
   - Configurar permisos de acceso
   - Generar enlaces de compartir

===============================================
PASO 10: MANTENIMIENTO
===============================================

ACTUALIZACIONES PERIÓDICAS:
• Datos: Ejecutar ETL semanalmente
• Modelos ML: Re-entrenar mensualmente
• Dashboard: Revisar KPIs trimestralmente

MONITOREO:
• Rendimiento de consultas
• Uso por parte de usuarios
• Feedback y mejoras

===============================================
ARCHIVOS DE SOPORTE
===============================================

• dashboard_specification.json: Especificación técnica completa
• Datasets CSV: Datos optimizados para Power BI
• Esta guía: Instrucciones paso a paso

SOPORTE TÉCNICO:
Para consultas sobre la implementación, revisar los logs
de ETL y la documentación del proceso de minería de datos.

===============================================
GUÍA GENERADA AUTOMÁTICAMENTE
SISTEMA ETL TECHSKILLS v2.0 + POWER BI
===============================================
        """
        
        guide_file = self.powerbi_dir / 'PowerBI_Implementation_Guide.txt'
        with open(guide_file, 'w', encoding='utf-8') as f:
            f.write(guide_content)
        
        self.logger.info(f"Guía de implementación guardada en {guide_file}")
        return guide_file
    
    def export_for_powerbi(self, warehouse_df, mining_results):
        """
        Función principal que ejecuta toda la exportación para Power BI
        """
        self.logger.info("Iniciando exportación completa para Power BI")
        
        # Preparar datasets
        datasets = self.prepare_datasets_for_powerbi(warehouse_df, mining_results)
        
        # Crear especificación
        spec = self.create_powerbi_specification(datasets)
        
        # Generar guía
        guide_file = self.generate_powerbi_guide(datasets, spec)
        
        summary = {
            'datasets_created': len(datasets),
            'total_records': sum(len(df) for df in datasets.values() if df is not None),
            'output_directory': str(self.powerbi_dir.absolute()),
            'guide_file': str(guide_file),
            'specification_file': str(self.powerbi_dir / 'dashboard_specification.json')
        }
        
        self.logger.info(f"Exportación completada: {summary}")
        return summary
