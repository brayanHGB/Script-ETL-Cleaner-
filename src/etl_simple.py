#!/usr/bin/env python3

import sys
import os
import logging
import pandas as pd
import json
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__)))

from config import ETLConfig
from limpieza import DataCleaner
from transformacion import DataTransformer
from validaciones import DataValidator
from warehouse import DataWarehouse
from data_mining import DataMiningAnalyzer
from powerbi_export import PowerBIExporter


def setup_simple_logging():
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, 'etl_simple_log.txt'), encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main_simple():
    setup_simple_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("*** INICIANDO PROCESO ETL - TECH SKILLS ***")
    
    try:
        config = ETLConfig()
        cleaner = DataCleaner(config)
        transformer = DataTransformer(config)
        validator = DataValidator(config)
        warehouse = DataWarehouse(config)
        data_mining = DataMiningAnalyzer(config)
        powerbi_exporter = PowerBIExporter(config)
        
        logger.info("PASO 1: Extracción de datos")
        tech_jobs_df = pd.read_csv(config.tech_jobs_file)
        tech_investment_df = pd.read_csv(config.tech_investment_file)
        skill_profiles_df = pd.read_csv(config.tech_skill_profiles_file)
        
        logger.info(f"Empleos tech cargados: {len(tech_jobs_df)} registros")
        logger.info(f"Inversiones tech cargadas: {len(tech_investment_df)} registros")
        logger.info(f"Perfiles de habilidades cargados: {len(skill_profiles_df)} registros")
        
        logger.info("PASO 2: Limpieza de datos")
        jobs_cleaning_config = {
            'text_columns': ['Empresa', 'Ciudad', 'País', 'Cargo'],
            'numeric_columns': ['Salario_Anual_USD'],
            'date_columns': ['Fecha_Publicación'],
            'missing_strategy': {
                'Ciudad': 'unknown',
                'País': 'unknown',
                'Salario_Anual_USD': 'median'
            }
        }
        tech_jobs_clean = cleaner.clean_dataframe(tech_jobs_df, jobs_cleaning_config)
        
        investment_cleaning_config = {
            'text_columns': ['Organización', 'Área_Tecnológica', 'Ciudad', 'País'],
            'numeric_columns': ['Inversión_USD', 'Participantes', 'Satisfacción_Promedio'],
            'missing_strategy': {
                'Ciudad': 'unknown',
                'País': 'unknown',
                'Inversión_USD': 'median'
            }
        }
        tech_investment_clean = cleaner.clean_dataframe(tech_investment_df, investment_cleaning_config)
        
        profiles_cleaning_config = {
            'text_columns': ['Ciudad', 'País', 'Lenguajes_Dominio', 'Frameworks_Dominio', 'Certificaciones', 'Nivel_Educativo', 'Área_Trabajo_Actual'],
            'numeric_columns': ['Edad', 'Años_Experiencia', 'Salario_Actual_USD'],
            'missing_strategy': {
                'Ciudad': 'unknown',
                'País': 'unknown',
                'Certificaciones': 'unknown',
                'Años_Experiencia': 'median',
                'Salario_Actual_USD': 'median'
            }
        }
        skill_profiles_clean = cleaner.clean_dataframe(skill_profiles_df, profiles_cleaning_config)
        
        logger.info("PASO 3: Transformación de datos")
        transformation_config = {
            'normalize_columns': True,
            'create_derived': True,
            'text_features': ['Cargo', 'Área_Tecnológica', 'Lenguajes_Dominio', 'Área_Trabajo_Actual']
        }
        tech_jobs_transformed = transformer.transform_dataframe(tech_jobs_clean, transformation_config)
        tech_investment_transformed = transformer.transform_dataframe(tech_investment_clean, transformation_config)
        skill_profiles_transformed = transformer.transform_dataframe(skill_profiles_clean, transformation_config)
        
        logger.info("PASO 4: Análisis de minería de datos")
        warehouse_temp_df, _ = warehouse.create_tech_warehouse(
            tech_jobs_transformed, 
            tech_investment_transformed, 
            skill_profiles_transformed
        )
        
        data_mining_results = data_mining.perform_data_mining_analysis(warehouse_temp_df)
        
        logger.info("PASO 5: Validación de calidad")
        validation_config = {
            'required_columns': tech_jobs_transformed.columns.tolist(),
            'business_rules': {
                'salary_positive': {
                    'type': 'range',
                    'column': 'salario_anual_usd' if 'salario_anual_usd' in tech_jobs_transformed.columns else 'Salario_Anual_USD',
                    'min': 0,
                    'max': 500000
                }
            }
        }
        
        jobs_validation = validator.validate_dataframe(tech_jobs_transformed, validation_config)
        investment_validation = validator.validate_dataframe(tech_investment_transformed)
        
        profiles_validation_config = {
            'required_columns': skill_profiles_transformed.columns.tolist(),
            'business_rules': {
                'age_realistic': {
                    'type': 'range',
                    'column': 'edad' if 'edad' in skill_profiles_transformed.columns else 'Edad',
                    'min': 18,
                    'max': 70
                },
                'experience_realistic': {
                    'type': 'range',
                    'column': 'años_experiencia' if 'años_experiencia' in skill_profiles_transformed.columns else 'Años_Experiencia',
                    'min': 0,
                    'max': 40
                }
            }
        }
        profiles_validation = validator.validate_dataframe(skill_profiles_transformed, profiles_validation_config)
        
        logger.info("PASO 6: Carga de datos procesados")
        tech_jobs_transformed['source'] = 'jobs'
        tech_investment_transformed['source'] = 'investment'
        skill_profiles_transformed['source'] = 'profiles'
        
        tech_jobs_transformed.to_csv(config.processed_data_dir / 'tech_jobs_processed.csv', index=False)
        tech_investment_transformed.to_csv(config.processed_data_dir / 'tech_investment_processed.csv', index=False)
        skill_profiles_transformed.to_csv(config.processed_data_dir / 'skill_profiles_processed.csv', index=False)
        
        logger.info("PASO 7: Generación de TechWarehouse consolidado")
        warehouse_df, warehouse_summary = warehouse.create_tech_warehouse(
            tech_jobs_transformed, 
            tech_investment_transformed, 
            skill_profiles_transformed
        )
        
        logger.info("PASO 8: Exportación para Power BI")
        powerbi_summary = powerbi_exporter.export_for_powerbi(warehouse_df, data_mining_results)
        
        summary_data = {
            'jobs_records': len(tech_jobs_transformed),
            'investment_records': len(tech_investment_transformed),
            'profiles_records': len(skill_profiles_transformed),
            'total_records': len(tech_jobs_transformed) + len(tech_investment_transformed) + len(skill_profiles_transformed),
            'processing_date': pd.Timestamp.now().isoformat(),
            'data_quality_score': (jobs_validation.get('completeness_validation', {}).get('completeness_score', 0) + 
                                 investment_validation.get('completeness_validation', {}).get('completeness_score', 0) + 
                                 profiles_validation.get('completeness_validation', {}).get('completeness_score', 0)) / 3
        }
        
        etl_metrics = {
            'fecha_ejecucion': pd.Timestamp.now().isoformat(),
            'registros_procesados': {
                'empleos_tech': summary_data['jobs_records'],
                'inversiones_tech': summary_data['investment_records'],
                'perfiles_habilidades': summary_data['profiles_records'],
                'total_registros': summary_data['total_records'],
                'warehouse_consolidado': len(warehouse_df),
                'fecha_procesamiento': summary_data['processing_date'],
                'puntuacion_calidad_datos': round(summary_data['data_quality_score'], 2)
            },
            'resultados_validacion': {
                'empleos': jobs_validation.get('overall_status', 'unknown'),
                'inversiones': investment_validation.get('overall_status', 'unknown'),
                'perfiles': profiles_validation.get('overall_status', 'unknown')
            },
            'warehouse_consolidado': warehouse_summary,
            'analisis_mineria_datos': data_mining_results,
            'exportacion_powerbi': powerbi_summary,
            'resumen_calidad': {
                'estado_general': 'EXCELENTE' if summary_data['data_quality_score'] > 90 else 
                                'BUENO' if summary_data['data_quality_score'] > 80 else 
                                'REGULAR' if summary_data['data_quality_score'] > 70 else 'NECESITA_MEJORA',
                'duplicados_eliminados': {
                    'empleos': len(tech_jobs_df) - len(tech_jobs_clean),
                    'inversiones': len(tech_investment_df) - len(tech_investment_clean),
                    'perfiles': len(skill_profiles_df) - len(skill_profiles_clean)
                }
            }
        }
        
        with open(config.output_dir / 'metricas_etl_simple.json', 'w', encoding='utf-8') as f:
            json.dump({'metricas_etl': etl_metrics}, f, indent=2, ensure_ascii=False)
        
        logger.info("*** PROCESO ETL COMPLETADO EXITOSAMENTE ***")
        logger.info(f"Total de registros procesados: {summary_data['total_records']:,}")
        logger.info(f"TechWarehouse consolidado: {len(warehouse_df):,} registros")
        logger.info(f"Puntuacion de calidad de datos: {summary_data['data_quality_score']:.2f}%")
        
        mining_success = data_mining_results.get('resumen_ejecucion', {}).get('analisis_exitosos', 0)
        logger.info(f"Análisis de minería de datos: {mining_success}/4 análisis completados exitosamente")
        
        powerbi_datasets = powerbi_summary.get('datasets_created', 0)
        logger.info(f"Exportación Power BI: {powerbi_datasets} datasets generados para dashboard")
        
        quality_status = etl_metrics['resumen_calidad']['estado_general']
        status_messages = {
            'EXCELENTE': 'Calidad EXCELENTE - Datos listos para analisis',
            'BUENO': 'Calidad BUENA - Datos confiables',
            'REGULAR': 'Calidad REGULAR - Revisar advertencias',
            'NECESITA_MEJORA': 'Calidad BAJA - Requiere atencion'
        }
        
        logger.info(f"RESULTADO: {status_messages.get(quality_status, 'Estado desconocido')}")
        
        return True
        
    except Exception as e:
        logger.error(f"ERROR en el proceso ETL: {str(e)}")
        return False


if __name__ == "__main__":
    success = main_simple()
    if success:
        print("\n" + "="*50)
        print("  ETL COMPLETADO - Revisa los archivos generados")
        print("="*50)
    else:
        print("\n" + "="*50)
        print("  ETL FALLÓ - Revisa los logs para detalles")
        print("="*50)
