#!/usr/bin/env python3
"""
ETL Main Script for Tech Skills Project
This is the main entry point for the ETL process
"""

import sys
import os
import logging
import pandas as pd
import json
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from config import ETLConfig
from limpieza import DataCleaner
from transformacion import DataTransformer
from validaciones import DataValidator


def setup_logging():
    """Setup logging configuration"""
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, 'etl_log.txt')),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Main ETL process"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Iniciando proceso ETL para el Proyecto Tech Skills")
    
    try:
        # Initialize components
        config = ETLConfig()
        cleaner = DataCleaner(config)
        transformer = DataTransformer(config)
        validator = DataValidator(config)
        
        # ETL Process
        logger.info("📂 Paso 1: Extracción de Datos")
        # Load raw data
        import pandas as pd
        
        tech_jobs_df = pd.read_csv(config.tech_jobs_file)
        tech_investment_df = pd.read_csv(config.tech_investment_file)
        
        logger.info(f"✅ Cargados {len(tech_jobs_df)} registros de empleos tech")
        logger.info(f"✅ Cargados {len(tech_investment_df)} registros de inversión tech")
        
        logger.info("🧹 Paso 2: Limpieza de Datos")
        # Clean tech jobs data
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
        
        # Clean investment data
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
        
        logger.info("🔄 Paso 3: Transformación de Datos")
        # Transform data
        transformation_config = {
            'normalize_columns': True,
            'create_derived': True,
            'text_features': ['Cargo', 'Área_Tecnológica']
        }
        tech_jobs_transformed = transformer.transform_dataframe(tech_jobs_clean, transformation_config)
        tech_investment_transformed = transformer.transform_dataframe(tech_investment_clean, transformation_config)
        
        logger.info("✔️ Paso 4: Validación de Calidad")
        # Validate data quality
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
        
        logger.info("💾 Paso 5: Carga de Datos")
        # Combine and save processed data
        tech_jobs_transformed['source'] = 'jobs'
        tech_investment_transformed['source'] = 'investment'
        
        # Save individual processed files
        tech_jobs_transformed.to_csv(config.processed_data_dir / 'tech_jobs_processed.csv', index=False)
        tech_investment_transformed.to_csv(config.processed_data_dir / 'tech_investment_processed.csv', index=False)
        
        # Create summary warehouse file
        summary_data = {
            'jobs_records': len(tech_jobs_transformed),
            'investment_records': len(tech_investment_transformed),
            'total_records': len(tech_jobs_transformed) + len(tech_investment_transformed),
            'processing_date': pd.Timestamp.now().isoformat(),
            'data_quality_score': (jobs_validation.get('completeness_validation', {}).get('completeness_score', 0) + 
                                 investment_validation.get('completeness_validation', {}).get('completeness_score', 0)) / 2
        }
        
        # Save summary in Spanish
        import json
        etl_metrics = {
            'fecha_ejecucion': pd.Timestamp.now().isoformat(),
            'registros_procesados': {
                'empleos_tech': summary_data['jobs_records'],
                'inversiones_tech': summary_data['investment_records'],
                'total_registros': summary_data['total_records'],
                'fecha_procesamiento': summary_data['processing_date'],
                'puntuacion_calidad_datos': round(summary_data['data_quality_score'], 2)
            },
            'resultados_validacion': {
                'empleos': jobs_validation.get('overall_status', 'unknown'),
                'inversiones': investment_validation.get('overall_status', 'unknown')
            },
            'resumen_calidad': {
                'estado_general': 'EXCELENTE' if summary_data['data_quality_score'] > 90 else 
                                'BUENO' if summary_data['data_quality_score'] > 80 else 
                                'REGULAR' if summary_data['data_quality_score'] > 70 else 'NECESITA_MEJORA',
                'duplicados_eliminados': {
                    'empleos': len(tech_jobs_df) - len(tech_jobs_clean),
                    'inversiones': len(tech_investment_df) - len(tech_investment_clean)
                }
            }
        }
        
        with open(config.metrics_file, 'w', encoding='utf-8') as f:
            json.dump({'metricas_etl': etl_metrics}, f, indent=2, ensure_ascii=False)
        
        logger.info("🎉 ¡Proceso ETL completado exitosamente!")
        logger.info(f"📊 Total de registros procesados: {summary_data['total_records']:,}")
        logger.info(f"🏆 Puntuación de calidad de datos: {summary_data['data_quality_score']:.2f}%")
        
        # Status summary in Spanish
        quality_status = etl_metrics['resumen_calidad']['estado_general']
        status_messages = {
            'EXCELENTE': '🌟 Calidad EXCELENTE - Datos listos para análisis',
            'BUENO': '👍 Calidad BUENA - Datos confiables',
            'REGULAR': '⚠️  Calidad REGULAR - Revisar advertencias',
            'NECESITA_MEJORA': '🚨 Calidad BAJA - Requiere atención'
        }
        
        logger.info(f"📈 {status_messages.get(quality_status, 'Estado desconocido')}")
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso ETL: {str(e)}")
        raise


if __name__ == "__main__":
    main()
