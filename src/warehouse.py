import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any
from datetime import datetime


class DataWarehouse:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def standardize_common_columns(self, df: pd.DataFrame, source_type: str) -> pd.DataFrame:
        df = df.copy()
        
        common_mapping = {
            'ciudad': 'ciudad',
            'pais': 'pais', 
            'país': 'pais'
        }
        
        for old_col, new_col in common_mapping.items():
            if old_col in df.columns and old_col != new_col:
                df[new_col] = df[old_col]
                df.drop(columns=[old_col], inplace=True)
        
        df['fuente_datos'] = source_type
        df['fecha_procesamiento'] = pd.Timestamp.now()
        
        return df
    
    def create_unified_schema(self, jobs_df: pd.DataFrame, investment_df: pd.DataFrame, 
                            profiles_df: pd.DataFrame) -> pd.DataFrame:
        
        jobs_std = self.standardize_common_columns(jobs_df, 'empleos_tech')
        investment_std = self.standardize_common_columns(investment_df, 'inversiones_tech')
        profiles_std = self.standardize_common_columns(profiles_df, 'perfiles_habilidades')
        
        base_columns = ['fuente_datos', 'fecha_procesamiento', 'ciudad', 'pais']
        
        consolidated_records = []
        
        for _, row in jobs_std.iterrows():
            record = {col: None for col in base_columns}
            record.update({
                'fuente_datos': 'empleos_tech',
                'fecha_procesamiento': row.get('fecha_procesamiento'),
                'ciudad': row.get('ciudad'),
                'pais': row.get('pais'),
                'id_registro': row.get('id_oferta'),
                'empresa_organizacion': row.get('empresa'),
                'cargo_area': row.get('cargo'),
                'tecnologia_principal': row.get('lenguaje'),
                'framework_herramienta': row.get('framework'),
                'nivel_experiencia': row.get('nivel_seniority'),
                'salario_usd': row.get('salario_anual_usd'),
                'modalidad_tipo': row.get('modalidad'),
                'fecha_referencia': row.get('fecha_publicacion'),
                'anio_referencia': row.get('fecha_publicacion_year') if 'fecha_publicacion_year' in row else None
            })
            consolidated_records.append(record)
        
        for _, row in investment_std.iterrows():
            record = {col: None for col in base_columns}
            record.update({
                'fuente_datos': 'inversiones_tech',
                'fecha_procesamiento': row.get('fecha_procesamiento'),
                'ciudad': row.get('ciudad'),
                'pais': row.get('pais'),
                'id_registro': row.get('id_programa'),
                'empresa_organizacion': row.get('organizacion'),
                'cargo_area': row.get('area_tecnologica'),
                'inversion_usd': row.get('inversion_usd'),
                'participantes': row.get('participantes'),
                'duracion_meses': row.get('duracion_meses'),
                'satisfaccion_promedio': row.get('satisfaccion_promedio'),
                'anio_referencia': row.get('ano') if 'ano' in row else row.get('año')
            })
            consolidated_records.append(record)
        
        for _, row in profiles_std.iterrows():
            record = {col: None for col in base_columns}
            record.update({
                'fuente_datos': 'perfiles_habilidades',
                'fecha_procesamiento': row.get('fecha_procesamiento'),
                'ciudad': row.get('ciudad'),
                'pais': row.get('pais'),
                'id_registro': row.get('id_persona'),
                'edad': row.get('edad'),
                'tecnologia_principal': row.get('lenguajes_dominio'),
                'framework_herramienta': row.get('frameworks_dominio'),
                'certificaciones': row.get('certificaciones'),
                'anos_experiencia': row.get('anos_experiencia'),
                'nivel_educativo': row.get('nivel_educativo'),
                'cargo_area': row.get('area_trabajo_actual'),
                'salario_usd': row.get('salario_actual_usd')
            })
            consolidated_records.append(record)
        
        warehouse_df = pd.DataFrame(consolidated_records)
        
        column_order = [
            'fuente_datos', 'fecha_procesamiento', 'id_registro',
            'empresa_organizacion', 'cargo_area', 'ciudad', 'pais',
            'tecnologia_principal', 'framework_herramienta', 
            'nivel_experiencia', 'salario_usd', 'modalidad_tipo',
            'fecha_referencia', 'anio_referencia', 'edad', 'anos_experiencia',
            'nivel_educativo', 'certificaciones', 'inversion_usd',
            'participantes', 'duracion_meses', 'satisfaccion_promedio'
        ]
        
        for col in column_order:
            if col not in warehouse_df.columns:
                warehouse_df[col] = None
        
        warehouse_df = warehouse_df[column_order]
        
        self.logger.info(f"Created consolidated warehouse with {len(warehouse_df)} total records")
        return warehouse_df
    
    def add_derived_metrics(self, warehouse_df: pd.DataFrame) -> pd.DataFrame:
        
        warehouse_df['tiene_salario'] = warehouse_df['salario_usd'].notna()
        warehouse_df['rango_salario'] = pd.cut(
            warehouse_df['salario_usd'].fillna(0), 
            bins=[0, 50000, 75000, 100000, 150000, np.inf],
            labels=['<50K', '50K-75K', '75K-100K', '100K-150K', '150K+']
        )
        
        warehouse_df['tiene_experiencia'] = warehouse_df['anos_experiencia'].notna()
        warehouse_df['nivel_experiencia_grupo'] = pd.cut(
            warehouse_df['anos_experiencia'].fillna(0),
            bins=[0, 2, 5, 10, np.inf],
            labels=['Junior', 'Mid', 'Senior', 'Expert']
        )
        
        warehouse_df['pais_normalizado'] = warehouse_df['pais'].str.title().fillna('No Especificado')
        warehouse_df['ciudad_normalizada'] = warehouse_df['ciudad'].str.title().fillna('No Especificado')
        
        warehouse_df['mes_procesamiento'] = warehouse_df['fecha_procesamiento'].dt.month
        warehouse_df['ano_procesamiento'] = warehouse_df['fecha_procesamiento'].dt.year
        
        self.logger.info("Added derived metrics to warehouse")
        return warehouse_df
    
    def generate_warehouse_summary(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        
        summary = {
            'resumen_general': {
                'total_registros': int(len(warehouse_df)),
                'fecha_generacion': pd.Timestamp.now().isoformat(),
                'fuentes_datos': {k: int(v) for k, v in warehouse_df['fuente_datos'].value_counts().to_dict().items()}
            },
            'distribucion_geografica': {
                'paises_unicos': int(warehouse_df['pais_normalizado'].nunique()),
                'ciudades_unicas': int(warehouse_df['ciudad_normalizada'].nunique()),
                'top_paises': {k: int(v) for k, v in warehouse_df['pais_normalizado'].value_counts().head(5).to_dict().items()},
                'top_ciudades': {k: int(v) for k, v in warehouse_df['ciudad_normalizada'].value_counts().head(5).to_dict().items()}
            },
            'metricas_salariales': {
                'registros_con_salario': int(warehouse_df['tiene_salario'].sum()),
                'salario_promedio': float(warehouse_df['salario_usd'].mean()) if warehouse_df['salario_usd'].notna().any() else 0,
                'salario_mediano': float(warehouse_df['salario_usd'].median()) if warehouse_df['salario_usd'].notna().any() else 0,
                'distribucion_rangos': {str(k): int(v) for k, v in warehouse_df['rango_salario'].value_counts().to_dict().items() if pd.notna(k)}
            },
            'metricas_experiencia': {
                'registros_con_experiencia': int(warehouse_df['tiene_experiencia'].sum()),
                'experiencia_promedio': float(warehouse_df['anos_experiencia'].mean()) if warehouse_df['anos_experiencia'].notna().any() else 0,
                'distribucion_niveles': {str(k): int(v) for k, v in warehouse_df['nivel_experiencia_grupo'].value_counts().to_dict().items() if pd.notna(k)}
            },
            'tecnologias_populares': {
                'lenguajes': {str(k): int(v) for k, v in warehouse_df['tecnologia_principal'].value_counts().head(10).to_dict().items() if pd.notna(k)},
                'frameworks': {str(k): int(v) for k, v in warehouse_df['framework_herramienta'].value_counts().head(10).to_dict().items() if pd.notna(k)}
            }
        }
        
        return summary
    
    def create_tech_warehouse(self, jobs_df: pd.DataFrame, investment_df: pd.DataFrame, 
                            profiles_df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
        
        self.logger.info("Starting TechWarehouse generation")
        
        warehouse_df = self.create_unified_schema(jobs_df, investment_df, profiles_df)
        
        warehouse_df = self.add_derived_metrics(warehouse_df)
        
        warehouse_df.to_csv(self.config.warehouse_file, index=False, encoding='utf-8')
        self.logger.info(f"TechWarehouse saved to {self.config.warehouse_file}")
        
        summary = self.generate_warehouse_summary(warehouse_df)
        
        return warehouse_df, summary
