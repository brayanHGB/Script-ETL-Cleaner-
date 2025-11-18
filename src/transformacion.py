import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta


class DataTransformer:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        original_columns = df.columns.tolist()
        
        df.columns = (df.columns
                     .str.lower()
                     .str.replace(r'[^\w\s]', '', regex=True)
                     .str.replace(r'\s+', '_', regex=True)
                     .str.strip('_'))
        
        self.logger.info(f"Normalized column names: {dict(zip(original_columns, df.columns))}")
        return df
    
    def create_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        
        if 'age' in df.columns:
            df['age_group'] = pd.cut(df['age'], 
                                   bins=[0, 25, 35, 45, 55, 100], 
                                   labels=['18-25', '26-35', '36-45', '46-55', '55+'])
            self.logger.info("Created age_group column")
        
        if 'salary' in df.columns:
            df['salary_range'] = pd.cut(df['salary'], 
                                      bins=[0, 50000, 75000, 100000, 150000, np.inf], 
                                      labels=['<50K', '50K-75K', '75K-100K', '100K-150K', '150K+'])
            self.logger.info("Created salary_range column")
        
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[f'{col}_year'] = df[col].dt.year
            df[f'{col}_month'] = df[col].dt.month
            df[f'{col}_quarter'] = df[col].dt.quarter
            self.logger.info(f"Created date-based columns for {col}")
        
        return df
    
    def encode_categorical_variables(self, df: pd.DataFrame, encoding_config: Dict = None) -> pd.DataFrame:
        """Encode categorical variables"""
        if encoding_config is None:
            encoding_config = {}
        
        for column, method in encoding_config.items():
            if column not in df.columns:
                self.logger.warning(f"Column {column} not found for encoding")
                continue
            
            if method == 'one_hot':
                dummies = pd.get_dummies(df[column], prefix=column, drop_first=True)
                df = pd.concat([df, dummies], axis=1)
                self.logger.info(f"Applied one-hot encoding to {column}")
                
            elif method == 'label':
                df[f'{column}_encoded'] = df[column].astype('category').cat.codes
                self.logger.info(f"Applied label encoding to {column}")
                
            elif method == 'target':
                target_col = encoding_config.get('target_column')
                if target_col and target_col in df.columns:
                    target_mean = df.groupby(column)[target_col].mean()
                    df[f'{column}_target_encoded'] = df[column].map(target_mean)
                    self.logger.info(f"Applied target encoding to {column}")
        
        return df
    
    def aggregate_data(self, df: pd.DataFrame, group_by: List[str], aggregations: Dict) -> pd.DataFrame:
        if not all(col in df.columns for col in group_by):
            missing_cols = [col for col in group_by if col not in df.columns]
            self.logger.error(f"Missing columns for grouping: {missing_cols}")
            return df
        
        try:
            aggregated_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            if isinstance(aggregated_df.columns, pd.MultiIndex):
                aggregated_df.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                       for col in aggregated_df.columns.values]
            
            self.logger.info(f"Aggregated data by {group_by}")
            return aggregated_df
            
        except Exception as e:
            self.logger.error(f"Error in aggregation: {str(e)}")
            return df
    
    def normalize_numeric_columns(self, df: pd.DataFrame, columns: List[str], method: str = 'min_max') -> pd.DataFrame:
        """Normalize numeric columns"""
        for column in columns:
            if column not in df.columns:
                self.logger.warning(f"Column {column} not found for normalization")
                continue
            
            if df[column].dtype not in ['int64', 'float64']:
                self.logger.warning(f"Column {column} is not numeric")
                continue
            
            if method == 'min_max':
                # Min-max normalization (0-1)
                min_val = df[column].min()
                max_val = df[column].max()
                df[f'{column}_normalized'] = (df[column] - min_val) / (max_val - min_val)
                
            elif method == 'z_score':
                # Z-score normalization
                mean_val = df[column].mean()
                std_val = df[column].std()
                df[f'{column}_normalized'] = (df[column] - mean_val) / std_val
                
            elif method == 'robust':
                # Robust normalization using median and IQR
                median_val = df[column].median()
                q75 = df[column].quantile(0.75)
                q25 = df[column].quantile(0.25)
                iqr = q75 - q25
                df[f'{column}_normalized'] = (df[column] - median_val) / iqr
            
            self.logger.info(f"Normalized column {column} using {method} method")
        
        return df
    
    def bin_continuous_variables(self, df: pd.DataFrame, binning_config: Dict) -> pd.DataFrame:
        """Bin continuous variables into categories"""
        for column, config in binning_config.items():
            if column not in df.columns:
                self.logger.warning(f"Column {column} not found for binning")
                continue
            
            bins = config.get('bins')
            labels = config.get('labels')
            method = config.get('method', 'cut')
            
            try:
                if method == 'cut':
                    df[f'{column}_binned'] = pd.cut(df[column], bins=bins, labels=labels, include_lowest=True)
                elif method == 'qcut':
                    df[f'{column}_binned'] = pd.qcut(df[column], q=bins, labels=labels, duplicates='drop')
                
                self.logger.info(f"Binned column {column}")
                
            except Exception as e:
                self.logger.error(f"Error binning column {column}: {str(e)}")
        
        return df
    
    def merge_datasets(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                      merge_config: Dict) -> pd.DataFrame:
        """Merge two datasets"""
        try:
            left_on = merge_config.get('left_on')
            right_on = merge_config.get('right_on')
            how = merge_config.get('how', 'inner')
            suffixes = merge_config.get('suffixes', ('_x', '_y'))
            
            merged_df = pd.merge(df1, df2, 
                               left_on=left_on, 
                               right_on=right_on, 
                               how=how, 
                               suffixes=suffixes)
            
            self.logger.info(f"Merged datasets: {len(df1)} + {len(df2)} -> {len(merged_df)} records")
            return merged_df
            
        except Exception as e:
            self.logger.error(f"Error merging datasets: {str(e)}")
            return df1
    
    def create_features_from_text(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """Create features from text columns"""
        if text_column not in df.columns:
            self.logger.warning(f"Text column {text_column} not found")
            return df
        
        # Text length
        df[f'{text_column}_length'] = df[text_column].astype(str).str.len()
        
        # Word count
        df[f'{text_column}_word_count'] = df[text_column].astype(str).str.split().str.len()
        
        # Contains specific keywords (example for tech skills)
        tech_keywords = ['python', 'java', 'javascript', 'sql', 'machine learning', 'ai', 'data science']
        for keyword in tech_keywords:
            df[f'has_{keyword.replace(" ", "_")}'] = df[text_column].astype(str).str.lower().str.contains(keyword, na=False)
        
        self.logger.info(f"Created text features for column {text_column}")
        return df
    
    def pivot_data(self, df: pd.DataFrame, pivot_config: Dict) -> pd.DataFrame:
        """Pivot data for analysis"""
        try:
            index = pivot_config.get('index')
            columns = pivot_config.get('columns')
            values = pivot_config.get('values')
            aggfunc = pivot_config.get('aggfunc', 'mean')
            
            pivoted_df = df.pivot_table(index=index, 
                                      columns=columns, 
                                      values=values, 
                                      aggfunc=aggfunc, 
                                      fill_value=0)
            
            # Flatten column names if multi-level
            if isinstance(pivoted_df.columns, pd.MultiIndex):
                pivoted_df.columns = ['_'.join(map(str, col)).strip() for col in pivoted_df.columns.values]
            
            pivoted_df = pivoted_df.reset_index()
            self.logger.info("Pivoted data successfully")
            return pivoted_df
            
        except Exception as e:
            self.logger.error(f"Error pivoting data: {str(e)}")
            return df
    
    def transform_dataframe(self, df: pd.DataFrame, transformation_config: Dict = None) -> pd.DataFrame:
        """Main transformation function"""
        self.logger.info("Starting data transformation process")
        
        if transformation_config is None:
            transformation_config = {}
        
        # Normalize column names
        if transformation_config.get('normalize_columns', True):
            df = self.normalize_column_names(df)
        
        # Create derived columns
        if transformation_config.get('create_derived', True):
            df = self.create_derived_columns(df)
        
        # Encode categorical variables
        encoding_config = transformation_config.get('encoding', {})
        if encoding_config:
            df = self.encode_categorical_variables(df, encoding_config)
        
        # Normalize numeric columns
        normalization_config = transformation_config.get('normalization', {})
        if normalization_config:
            columns = normalization_config.get('columns', [])
            method = normalization_config.get('method', 'min_max')
            df = self.normalize_numeric_columns(df, columns, method)
        
        # Bin continuous variables
        binning_config = transformation_config.get('binning', {})
        if binning_config:
            df = self.bin_continuous_variables(df, binning_config)
        
        # Create text features
        text_columns = transformation_config.get('text_features', [])
        for col in text_columns:
            df = self.create_features_from_text(df, col)
        
        self.logger.info("Data transformation process completed")
        return df
