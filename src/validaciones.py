import pandas as pd
import numpy as np
import logging
import json
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime


class DataValidator:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.validation_results = {}
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        results = {
            'status': 'passed',
            'errors': [],
            'warnings': []
        }
        
        missing_columns = set(expected_schema.keys()) - set(df.columns)
        if missing_columns:
            results['status'] = 'failed'
            results['errors'].append(f"Missing columns: {list(missing_columns)}")
        
        extra_columns = set(df.columns) - set(expected_schema.keys())
        if extra_columns:
            results['warnings'].append(f"Extra columns found: {list(extra_columns)}")
        
        for column, expected_type in expected_schema.items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if expected_type not in actual_type and not self._is_compatible_type(actual_type, expected_type):
                    results['warnings'].append(f"Column {column}: expected {expected_type}, got {actual_type}")
        
        self.logger.info(f"Schema validation completed with status: {results['status']}")
        return results
    
    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        type_mappings = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32'],
            'string': ['object', 'string'],
            'datetime': ['datetime64', 'datetime64[ns]'],
            'bool': ['bool']
        }
        
        for type_group, compatible_types in type_mappings.items():
            if expected in type_group and actual in compatible_types:
                return True
        return False
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data quality metrics"""
        results = {
            'total_records': len(df),
            'columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'quality_checks': {}
        }
        
        # Check for minimum records threshold
        if len(df) < self.config.min_records_threshold:
            results['quality_checks']['min_records'] = {
                'status': 'failed',
                'message': f"Dataset has {len(df)} records, minimum required: {self.config.min_records_threshold}"
            }
        else:
            results['quality_checks']['min_records'] = {
                'status': 'passed',
                'message': f"Dataset has sufficient records: {len(df)}"
            }
        
        # Check for missing values
        missing_stats = {}
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            
            missing_stats[column] = {
                'count': int(missing_count),
                'percentage': round(missing_percentage, 2)
            }
            
            # Flag columns with high missing percentages
            if missing_percentage > self.config.max_null_percentage:
                results['quality_checks'][f'{column}_missing'] = {
                    'status': 'warning',
                    'message': f"Column {column} has {missing_percentage:.2f}% missing values"
                }
        
        results['missing_values'] = missing_stats
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100
        results['duplicates'] = {
            'count': int(duplicate_count),
            'percentage': round(duplicate_percentage, 2)
        }
        
        if duplicate_percentage > self.config.duplicate_threshold:
            results['quality_checks']['duplicates'] = {
                'status': 'warning',
                'message': f"Dataset has {duplicate_percentage:.2f}% duplicate records"
            }
        
        # Check for data consistency
        self._validate_data_consistency(df, results)
        
        self.logger.info("Data quality validation completed")
        return results
    
    def _validate_data_consistency(self, df: pd.DataFrame, results: Dict) -> None:
        """Validate data consistency rules"""
        consistency_checks = []
        
        # Example: Check if numeric columns have realistic ranges
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            min_val = df[col].min()
            max_val = df[col].max()
            
            # Check for negative values in columns that shouldn't have them
            if col.lower() in ['age', 'salary', 'experience', 'price', 'count'] and min_val < 0:
                consistency_checks.append({
                    'column': col,
                    'status': 'warning',
                    'message': f"Column {col} has negative values (min: {min_val})"
                })
            
            # Check for extremely large values that might be errors
            if max_val > 1e6 and col.lower() not in ['id', 'salary', 'revenue', 'investment']:
                consistency_checks.append({
                    'column': col,
                    'status': 'warning',
                    'message': f"Column {col} has very large values (max: {max_val})"
                })
        
        # Example: Check date consistency
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            min_date = df[col].min()
            max_date = df[col].max()
            
            # Check for future dates if they shouldn't exist
            if col.lower() in ['birth_date', 'hire_date'] and max_date > pd.Timestamp.now():
                consistency_checks.append({
                    'column': col,
                    'status': 'warning',
                    'message': f"Column {col} has future dates"
                })
            
            # Check for very old dates that might be errors
            if min_date < pd.Timestamp('1900-01-01'):
                consistency_checks.append({
                    'column': col,
                    'status': 'warning',
                    'message': f"Column {col} has very old dates (min: {min_date})"
                })
        
        results['consistency_checks'] = consistency_checks
    
    def validate_business_rules(self, df: pd.DataFrame, business_rules: Dict) -> Dict[str, Any]:
        """Validate business-specific rules"""
        results = {
            'status': 'passed',
            'rule_results': {}
        }
        
        for rule_name, rule_config in business_rules.items():
            try:
                rule_type = rule_config.get('type')
                
                if rule_type == 'range':
                    column = rule_config.get('column')
                    min_val = rule_config.get('min')
                    max_val = rule_config.get('max')
                    
                    if column in df.columns:
                        violations = df[(df[column] < min_val) | (df[column] > max_val)]
                        violation_count = len(violations)
                        
                        results['rule_results'][rule_name] = {
                            'type': 'range',
                            'violations': violation_count,
                            'percentage': round((violation_count / len(df)) * 100, 2),
                            'status': 'passed' if violation_count == 0 else 'failed'
                        }
                
                elif rule_type == 'categorical':
                    column = rule_config.get('column')
                    valid_values = rule_config.get('valid_values', [])
                    
                    if column in df.columns:
                        violations = df[~df[column].isin(valid_values)]
                        violation_count = len(violations)
                        
                        results['rule_results'][rule_name] = {
                            'type': 'categorical',
                            'violations': violation_count,
                            'percentage': round((violation_count / len(df)) * 100, 2),
                            'status': 'passed' if violation_count == 0 else 'failed',
                            'invalid_values': df[~df[column].isin(valid_values)][column].unique().tolist()
                        }
                
                elif rule_type == 'relationship':
                    # Example: column1 > column2
                    column1 = rule_config.get('column1')
                    column2 = rule_config.get('column2')
                    operator = rule_config.get('operator', '>')
                    
                    if column1 in df.columns and column2 in df.columns:
                        if operator == '>':
                            violations = df[df[column1] <= df[column2]]
                        elif operator == '<':
                            violations = df[df[column1] >= df[column2]]
                        elif operator == '>=':
                            violations = df[df[column1] < df[column2]]
                        elif operator == '<=':
                            violations = df[df[column1] > df[column2]]
                        else:
                            violations = pd.DataFrame()
                        
                        violation_count = len(violations)
                        
                        results['rule_results'][rule_name] = {
                            'type': 'relationship',
                            'violations': violation_count,
                            'percentage': round((violation_count / len(df)) * 100, 2),
                            'status': 'passed' if violation_count == 0 else 'failed'
                        }
                
            except Exception as e:
                results['rule_results'][rule_name] = {
                    'status': 'error',
                    'message': f"Error validating rule: {str(e)}"
                }
                results['status'] = 'error'
        
        # Overall status
        failed_rules = [rule for rule, result in results['rule_results'].items() 
                       if result.get('status') == 'failed']
        if failed_rules:
            results['status'] = 'failed'
        
        self.logger.info(f"Business rules validation completed with status: {results['status']}")
        return results
    
    def validate_completeness(self, df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Validate data completeness"""
        results = {
            'status': 'passed',
            'completeness_score': 0,
            'column_completeness': {}
        }
        
        total_completeness = 0
        
        for column in required_columns:
            if column in df.columns:
                non_null_count = df[column].count()
                total_count = len(df)
                completeness = (non_null_count / total_count) * 100 if total_count > 0 else 0
                
                results['column_completeness'][column] = {
                    'completeness_percentage': round(completeness, 2),
                    'non_null_count': int(non_null_count),
                    'total_count': int(total_count)
                }
                
                total_completeness += completeness
                
                # Mark as failed if completeness is too low
                if completeness < 80:  # 80% threshold
                    results['status'] = 'warning'
            else:
                results['column_completeness'][column] = {
                    'completeness_percentage': 0,
                    'status': 'missing_column'
                }
                results['status'] = 'failed'
        
        # Calculate overall completeness score
        if required_columns:
            results['completeness_score'] = round(total_completeness / len(required_columns), 2)
        
        self.logger.info(f"Completeness validation completed with score: {results['completeness_score']}%")
        return results
    
    def generate_validation_report(self, df: pd.DataFrame, validation_config: Dict = None) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        self.logger.info("Generating validation report")
        
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'dataset_info': {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': df.columns.tolist()
            }
        }
        
        if validation_config is None:
            validation_config = {}
        
        # Schema validation
        expected_schema = validation_config.get('schema')
        if expected_schema:
            report['schema_validation'] = self.validate_schema(df, expected_schema)
        
        # Data quality validation
        report['quality_validation'] = self.validate_data_quality(df)
        
        # Business rules validation
        business_rules = validation_config.get('business_rules')
        if business_rules:
            report['business_rules_validation'] = self.validate_business_rules(df, business_rules)
        
        # Completeness validation
        required_columns = validation_config.get('required_columns', df.columns.tolist())
        report['completeness_validation'] = self.validate_completeness(df, required_columns)
        
        # Overall status
        statuses = []
        for validation_type, results in report.items():
            if isinstance(results, dict) and 'status' in results:
                statuses.append(results['status'])
        
        if 'failed' in statuses:
            report['overall_status'] = 'failed'
        elif 'warning' in statuses:
            report['overall_status'] = 'warning'
        else:
            report['overall_status'] = 'passed'
        
        self.validation_results = report
        self.logger.info(f"Validation report generated with overall status: {report['overall_status']}")
        return report
    
    def save_validation_report(self, report: Dict = None, file_path: str = None) -> None:
        """Save validation report to file"""
        if report is None:
            report = self.validation_results
        
        if file_path is None:
            file_path = self.config.quality_report_file
        
        # Traducción de estados
        status_translation = {
            'passed': 'APROBADO',
            'warning': 'ADVERTENCIA', 
            'failed': 'FALLIDO',
            'error': 'ERROR',
            'unknown': 'DESCONOCIDO'
        }
        
        try:
            # Create a readable text report in Spanish
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write("REPORTE DE VALIDACIÓN DE CALIDAD DE DATOS\n")
                f.write("=" * 55 + "\n\n")
                f.write(f"Fecha de Validación: {report.get('validation_timestamp', 'Desconocida')}\n")
                overall_status = report.get('overall_status', 'unknown')
                f.write(f"Estado General: {status_translation.get(overall_status, overall_status)}\n\n")
                
                # Dataset info
                dataset_info = report.get('dataset_info', {})
                f.write("INFORMACIÓN DEL CONJUNTO DE DATOS\n")
                f.write("-" * 35 + "\n")
                f.write(f"Filas: {dataset_info.get('rows', 'Desconocido')}\n")
                f.write(f"Columnas: {dataset_info.get('columns', 'Desconocido')}\n\n")
                
                # Quality validation
                quality_validation = report.get('quality_validation', {})
                f.write("VERIFICACIONES DE CALIDAD DE DATOS\n")
                f.write("-" * 35 + "\n")
                f.write(f"Total de Registros: {quality_validation.get('total_records', 'Desconocido')}\n")
                memory_usage = quality_validation.get('memory_usage_mb', 0)
                f.write(f"Uso de Memoria (MB): {memory_usage:.2f}\n")
                
                # Missing values
                missing_values = quality_validation.get('missing_values', {})
                if missing_values:
                    f.write("\nValores Faltantes por Columna:\n")
                    for col, stats in missing_values.items():
                        f.write(f"  {col}: {stats['count']} ({stats['percentage']}%)\n")
                
                # Duplicates
                duplicates = quality_validation.get('duplicates', {})
                f.write(f"\nRegistros Duplicados: {duplicates.get('count', 0)} ({duplicates.get('percentage', 0)}%)\n")
                
                # Quality checks
                quality_checks = quality_validation.get('quality_checks', {})
                if quality_checks:
                    f.write("\nResultados de Verificaciones de Calidad:\n")
                    for check, result in quality_checks.items():
                        check_status = result.get('status', 'unknown')
                        translated_status = status_translation.get(check_status, check_status)
                        f.write(f"  {check}: {translated_status} - {result.get('message', '')}\n")
                
                # Completeness validation
                completeness_validation = report.get('completeness_validation', {})
                if completeness_validation:
                    f.write("\nANÁLISIS DE COMPLETITUD\n")
                    f.write("-" * 25 + "\n")
                    completeness_score = completeness_validation.get('completeness_score', 0)
                    f.write(f"Puntuación de Completitud: {completeness_score}%\n")
                    
                    column_completeness = completeness_validation.get('column_completeness', {})
                    if column_completeness:
                        f.write("\nCompletitud por Columna:\n")
                        for col, stats in column_completeness.items():
                            if isinstance(stats, dict) and 'completeness_percentage' in stats:
                                f.write(f"  {col}: {stats['completeness_percentage']}%\n")
                
                # Business rules validation
                business_validation = report.get('business_rules_validation', {})
                if business_validation:
                    f.write("\nVALIDACIÓN DE REGLAS DE NEGOCIO\n")
                    f.write("-" * 32 + "\n")
                    business_status = business_validation.get('status', 'unknown')
                    f.write(f"Estado: {status_translation.get(business_status, business_status)}\n")
                    
                    rule_results = business_validation.get('rule_results', {})
                    if rule_results:
                        f.write("\nResultados por Regla:\n")
                        for rule, result in rule_results.items():
                            rule_status = result.get('status', 'unknown')
                            f.write(f"  {rule}: {status_translation.get(rule_status, rule_status)}\n")
                            if 'violations' in result:
                                f.write(f"    Violaciones: {result['violations']}\n")
            
            self.logger.info(f"Reporte de validación guardado en {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error guardando reporte de validación: {str(e)}")
    
    def validate_dataframe(self, df: pd.DataFrame, validation_config: Dict = None) -> Dict[str, Any]:
        """Main validation function"""
        self.logger.info("Starting data validation process")
        
        # Generate comprehensive validation report
        report = self.generate_validation_report(df, validation_config)
        
        # Save report to file
        self.save_validation_report(report)
        
        self.logger.info("Data validation process completed")
        return report
