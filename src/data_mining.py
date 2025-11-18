import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import accuracy_score, r2_score, classification_report
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("Scikit-learn y mlxtend no están instalados. Se saltarán los análisis de minería de datos.")


class DataMiningAnalyzer:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.results = {}
        
    def analyze_associations(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        if not SKLEARN_AVAILABLE:
            self.logger.warning("Saltando análisis de asociaciones - librerías no disponibles")
            return {'error': 'Librerías no disponibles'}
            
        self.logger.info("Iniciando análisis de asociaciones entre lenguajes y frameworks")
        
        try:
            tech_data = warehouse_df[
                (warehouse_df['tecnologia_principal'].notna()) & 
                (warehouse_df['framework_herramienta'].notna())
            ].copy()
            
            if len(tech_data) < 10:
                return {'error': 'Datos insuficientes para análisis de asociaciones'}
            
            transactions = []
            for _, row in tech_data.iterrows():
                transaction = []
                if pd.notna(row['tecnologia_principal']) and row['tecnologia_principal'] != 'Unknown':
                    transaction.append(f"lang_{row['tecnologia_principal']}")
                if pd.notna(row['framework_herramienta']) and row['framework_herramienta'] != 'Unknown':
                    transaction.append(f"fw_{row['framework_herramienta']}")
                
                if len(transaction) > 0:
                    transactions.append(transaction)
            
            if len(transactions) < 10:
                return {'error': 'Transacciones insuficientes'}
            
            te = TransactionEncoder()
            te_ary = te.fit(transactions).transform(transactions)
            df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
            
            frequent_itemsets = apriori(df_encoded, min_support=0.1, use_colnames=True)
            
            if len(frequent_itemsets) == 0:
                return {'error': 'No se encontraron patrones frecuentes'}
            
            rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.5)
            
            top_rules = rules.nlargest(10, 'confidence') if len(rules) > 0 else pd.DataFrame()
            
            associations_summary = {
                'total_transacciones': len(transactions),
                'patrones_frecuentes': len(frequent_itemsets),
                'reglas_asociacion': len(rules),
                'top_combinaciones': []
            }
            
            if len(top_rules) > 0:
                for _, rule in top_rules.head(5).iterrows():
                    antecedent = list(rule['antecedents'])[0] if rule['antecedents'] else 'N/A'
                    consequent = list(rule['consequents'])[0] if rule['consequents'] else 'N/A'
                    
                    associations_summary['top_combinaciones'].append({
                        'si_usa': antecedent,
                        'entonces_usa': consequent,
                        'confianza': round(rule['confidence'], 3),
                        'soporte': round(rule['support'], 3)
                    })
            
            self.logger.info(f"Análisis de asociaciones completado: {len(rules)} reglas encontradas")
            return associations_summary
            
        except Exception as e:
            self.logger.error(f"Error en análisis de asociaciones: {str(e)}")
            return {'error': f'Error en análisis: {str(e)}'}
    
    def analyze_clustering(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        if not SKLEARN_AVAILABLE:
            self.logger.warning("Saltando análisis de clustering - librerías no disponibles")
            return {'error': 'Librerías no disponibles'}
            
        self.logger.info("Iniciando análisis de clustering de perfiles")
        
        try:
            profiles_data = warehouse_df[
                warehouse_df['fuente_datos'] == 'perfiles_habilidades'
            ].copy()
            
            if len(profiles_data) < 50:
                return {'error': 'Datos insuficientes para clustering'}
            
            features_for_clustering = []
            feature_names = []
            
            if 'edad' in profiles_data.columns:
                edad_filled = profiles_data['edad'].fillna(profiles_data['edad'].median())
                features_for_clustering.append(edad_filled.values)
                feature_names.append('edad')
            
            if 'salario_usd' in profiles_data.columns:
                salario_filled = profiles_data['salario_usd'].fillna(profiles_data['salario_usd'].median())
                features_for_clustering.append(salario_filled.values)
                feature_names.append('salario_usd')
            
            country_encoder = LabelEncoder()
            if 'pais_normalizado' in profiles_data.columns:
                pais_encoded = country_encoder.fit_transform(profiles_data['pais_normalizado'].fillna('Unknown'))
                features_for_clustering.append(pais_encoded)
                feature_names.append('pais')
            
            if len(features_for_clustering) < 2:
                return {'error': 'Características insuficientes para clustering'}
            
            X = np.column_stack(features_for_clustering)
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            n_clusters = min(5, len(profiles_data) // 20)
            if n_clusters < 2:
                n_clusters = 2
                
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(X_scaled)
            
            profiles_data['cluster'] = clusters
            
            cluster_analysis = {
                'num_clusters': n_clusters,
                'caracteristicas_usadas': feature_names,
                'distribucion_clusters': {},
                'centroides_clusters': {}
            }
            
            for i in range(n_clusters):
                cluster_data = profiles_data[profiles_data['cluster'] == i]
                cluster_size = len(cluster_data)
                
                cluster_analysis['distribucion_clusters'][f'Cluster_{i+1}'] = {
                    'cantidad_perfiles': int(cluster_size),
                    'porcentaje': round((cluster_size / len(profiles_data)) * 100, 2)
                }
                
                centroide_info = {}
                if 'edad' in feature_names:
                    centroide_info['edad_promedio'] = round(cluster_data['edad'].mean(), 2)
                if 'salario_usd' in feature_names:
                    centroide_info['salario_promedio'] = round(cluster_data['salario_usd'].mean(), 2)
                if 'pais' in feature_names:
                    pais_mas_comun = cluster_data['pais_normalizado'].mode()
                    centroide_info['pais_predominante'] = pais_mas_comun.iloc[0] if len(pais_mas_comun) > 0 else 'Unknown'
                
                cluster_analysis['centroides_clusters'][f'Cluster_{i+1}'] = centroide_info
            
            self.logger.info(f"Clustering completado: {n_clusters} clusters identificados")
            return cluster_analysis
            
        except Exception as e:
            self.logger.error(f"Error en análisis de clustering: {str(e)}")
            return {'error': f'Error en clustering: {str(e)}'}
    
    def analyze_regression(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        if not SKLEARN_AVAILABLE:
            self.logger.warning("Saltando análisis de regresión - librerías no disponibles")
            return {'error': 'Librerías no disponibles'}
            
        self.logger.info("Iniciando análisis de regresión para predicción de participación")
        
        try:
            investment_data = warehouse_df[
                (warehouse_df['fuente_datos'] == 'inversiones_tech') &
                (warehouse_df['participantes'].notna()) &
                (warehouse_df['inversion_usd'].notna())
            ].copy()
            
            if len(investment_data) < 20:
                return {'error': 'Datos insuficientes para regresión'}
            
            features = []
            feature_names = []
            
            if 'inversion_usd' in investment_data.columns:
                features.append(investment_data['inversion_usd'].values)
                feature_names.append('inversion_usd')
            
            if 'duracion_meses' in investment_data.columns:
                duracion_filled = investment_data['duracion_meses'].fillna(investment_data['duracion_meses'].median())
                features.append(duracion_filled.values)
                feature_names.append('duracion_meses')
            
            if 'satisfaccion_promedio' in investment_data.columns:
                satisfaccion_filled = investment_data['satisfaccion_promedio'].fillna(investment_data['satisfaccion_promedio'].mean())
                features.append(satisfaccion_filled.values)
                feature_names.append('satisfaccion_promedio')
            
            if len(features) == 0:
                return {'error': 'No hay características numéricas para regresión'}
            
            X = np.column_stack(features) if len(features) > 1 else features[0].reshape(-1, 1)
            y = investment_data['participantes'].values
            
            if len(X) < 10:
                return {'error': 'Datos insuficientes para entrenamiento'}
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
            
            lr_model = LinearRegression()
            lr_model.fit(X_train, y_train)
            
            y_pred = lr_model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            
            rf_model = RandomForestRegressor(n_estimators=50, random_state=42)
            rf_model.fit(X_train, y_train)
            y_pred_rf = rf_model.predict(X_test)
            r2_rf = r2_score(y_test, y_pred_rf)
            
            regression_analysis = {
                'modelo_lineal': {
                    'r2_score': round(r2, 4),
                    'precision': 'Alta' if r2 > 0.7 else 'Media' if r2 > 0.5 else 'Baja'
                },
                'modelo_random_forest': {
                    'r2_score': round(r2_rf, 4),
                    'precision': 'Alta' if r2_rf > 0.7 else 'Media' if r2_rf > 0.5 else 'Baja'
                },
                'mejor_modelo': 'Random Forest' if r2_rf > r2 else 'Regresión Lineal',
                'caracteristicas_importantes': []
            }
            
            if len(features) > 1 and hasattr(rf_model, 'feature_importances_'):
                importances = rf_model.feature_importances_
                for i, importance in enumerate(importances):
                    if i < len(feature_names):
                        regression_analysis['caracteristicas_importantes'].append({
                            'caracteristica': feature_names[i],
                            'importancia': round(importance, 4)
                        })
            
            self.logger.info(f"Análisis de regresión completado: R² = {max(r2, r2_rf):.4f}")
            return regression_analysis
            
        except Exception as e:
            self.logger.error(f"Error en análisis de regresión: {str(e)}")
            return {'error': f'Error en regresión: {str(e)}'}
    
    def analyze_classification(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        if not SKLEARN_AVAILABLE:
            self.logger.warning("Saltando análisis de clasificación - librerías no disponibles")
            return {'error': 'Librerías no disponibles'}
            
        self.logger.info("Iniciando análisis de clasificación para probabilidad de contratación")
        
        try:
            jobs_data = warehouse_df[
                (warehouse_df['fuente_datos'] == 'empleos_tech') &
                (warehouse_df['salario_usd'].notna())
            ].copy()
            
            if len(jobs_data) < 50:
                return {'error': 'Datos insuficientes para clasificación'}
            
            jobs_data['alta_demanda'] = (jobs_data['salario_usd'] > jobs_data['salario_usd'].median()).astype(int)
            
            features = []
            feature_names = []
            
            tech_encoder = LabelEncoder()
            if 'tecnologia_principal' in jobs_data.columns:
                tech_encoded = tech_encoder.fit_transform(jobs_data['tecnologia_principal'].fillna('Unknown'))
                features.append(tech_encoded)
                feature_names.append('tecnologia_principal')
            
            country_encoder = LabelEncoder()
            if 'pais_normalizado' in jobs_data.columns:
                country_encoded = country_encoder.fit_transform(jobs_data['pais_normalizado'].fillna('Unknown'))
                features.append(country_encoded)
                feature_names.append('pais')
            
            level_encoder = LabelEncoder()
            if 'nivel_experiencia' in jobs_data.columns:
                level_encoded = level_encoder.fit_transform(jobs_data['nivel_experiencia'].fillna('Unknown'))
                features.append(level_encoded)
                feature_names.append('nivel_experiencia')
            
            if len(features) == 0:
                return {'error': 'No hay características para clasificación'}
            
            X = np.column_stack(features) if len(features) > 1 else features[0].reshape(-1, 1)
            y = jobs_data['alta_demanda'].values
            
            if len(np.unique(y)) < 2:
                return {'error': 'No hay variabilidad suficiente en la variable objetivo'}
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
            
            rf_classifier = RandomForestClassifier(n_estimators=50, random_state=42)
            rf_classifier.fit(X_train, y_train)
            
            y_pred = rf_classifier.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            lr_classifier = LogisticRegression(random_state=42, max_iter=1000)
            lr_classifier.fit(X_train, y_train)
            y_pred_lr = lr_classifier.predict(X_test)
            accuracy_lr = accuracy_score(y_test, y_pred_lr)
            
            classification_analysis = {
                'objetivo': 'Predicción de empleos de alta demanda (salario > mediana)',
                'modelo_random_forest': {
                    'precision': round(accuracy, 4),
                    'calidad': 'Alta' if accuracy > 0.8 else 'Media' if accuracy > 0.6 else 'Baja'
                },
                'modelo_logistic_regression': {
                    'precision': round(accuracy_lr, 4),
                    'calidad': 'Alta' if accuracy_lr > 0.8 else 'Media' if accuracy_lr > 0.6 else 'Baja'
                },
                'mejor_modelo': 'Random Forest' if accuracy > accuracy_lr else 'Regresión Logística',
                'caracteristicas_importantes': []
            }
            
            if len(features) > 1 and hasattr(rf_classifier, 'feature_importances_'):
                importances = rf_classifier.feature_importances_
                for i, importance in enumerate(importances):
                    if i < len(feature_names):
                        classification_analysis['caracteristicas_importantes'].append({
                            'caracteristica': feature_names[i],
                            'importancia': round(importance, 4)
                        })
            
            self.logger.info(f"Análisis de clasificación completado: Precisión = {max(accuracy, accuracy_lr):.4f}")
            return classification_analysis
            
        except Exception as e:
            self.logger.error(f"Error en análisis de clasificación: {str(e)}")
            return {'error': f'Error en clasificación: {str(e)}'}
    
    def perform_data_mining_analysis(self, warehouse_df: pd.DataFrame) -> Dict[str, Any]:
        self.logger.info("Iniciando análisis completo de minería de datos")
        
        analysis_results = {
            'fecha_analisis': pd.Timestamp.now().isoformat(),
            'total_registros_analizados': len(warehouse_df),
            'analisis_realizados': {}
        }
        
        self.logger.info("1. Análisis de asociaciones (lenguajes y frameworks)")
        analysis_results['analisis_realizados']['asociaciones'] = self.analyze_associations(warehouse_df)
        
        self.logger.info("2. Análisis de clustering (perfiles)")
        analysis_results['analisis_realizados']['clustering'] = self.analyze_clustering(warehouse_df)
        
        self.logger.info("3. Análisis de regresión (predicción de participación)")
        analysis_results['analisis_realizados']['regresion'] = self.analyze_regression(warehouse_df)
        
        self.logger.info("4. Análisis de clasificación (probabilidad de contratación)")
        analysis_results['analisis_realizados']['clasificacion'] = self.analyze_classification(warehouse_df)
        
        successful_analyses = sum(1 for result in analysis_results['analisis_realizados'].values() 
                                if 'error' not in result)
        
        analysis_results['resumen_ejecucion'] = {
            'analisis_exitosos': successful_analyses,
            'total_analisis': 4,
            'tasa_exito': f"{(successful_analyses/4)*100:.1f}%"
        }
        
        self.logger.info(f"Análisis de minería de datos completado: {successful_analyses}/4 exitosos")
        return analysis_results
