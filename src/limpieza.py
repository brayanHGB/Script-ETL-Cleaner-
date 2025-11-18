import pandas as pd
import numpy as np
import logging
import re
from typing import Dict, List, Tuple, Optional


class DataCleaner:
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def clean_text_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        if column not in df.columns:
            self.logger.warning(f"Column {column} not found in dataframe")
            return df
            
        df[column] = df[column].astype(str)
        df[column] = df[column].str.strip()
        df[column] = df[column].str.replace(r'\s+', ' ', regex=True)
        df[column] = df[column].replace('nan', np.nan)
        
        self.logger.info(f"Cleaned text column: {column}")
        return df
    
    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        initial_count = len(df)
        df_cleaned = df.drop_duplicates(subset=subset)
        removed_count = initial_count - len(df_cleaned)
        
        self.logger.info(f"Removed {removed_count} duplicate rows")
        return df_cleaned
    
    def handle_missing_values(self, df: pd.DataFrame, strategy: Dict[str, str] = None) -> pd.DataFrame:
        if strategy is None:
            strategy = {}
        
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            
            if missing_percentage > self.config.max_null_percentage:
                self.logger.warning(f"Column {column} has {missing_percentage:.2f}% missing values")
            
            # Apply strategy based on column type and user preference
            col_strategy = strategy.get(column, 'default')
            
            if col_strategy == 'drop':
                df = df.dropna(subset=[column])
            elif col_strategy == 'mean' and df[column].dtype in ['int64', 'float64']:
                df[column].fillna(df[column].mean(), inplace=True)
            elif col_strategy == 'median' and df[column].dtype in ['int64', 'float64']:
                df[column].fillna(df[column].median(), inplace=True)
            elif col_strategy == 'mode':
                mode_value = df[column].mode()
                if len(mode_value) > 0:
                    df[column].fillna(mode_value[0], inplace=True)
            elif col_strategy == 'forward_fill':
                df[column].fillna(method='ffill', inplace=True)
            elif col_strategy == 'zero' and df[column].dtype in ['int64', 'float64']:
                df[column].fillna(0, inplace=True)
            elif col_strategy == 'unknown':
                df[column].fillna('Unknown', inplace=True)
        
        self.logger.info("Handled missing values")
        return df
    
    def clean_numeric_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        if column not in df.columns:
            self.logger.warning(f"Column {column} not found in dataframe")
            return df
        
        if df[column].dtype == 'object':
            df[column] = df[column].astype(str)
            df[column] = df[column].str.replace(r'[$,€£¥]', '', regex=True)
            df[column] = df[column].str.replace(r'[^\d.-]', '', regex=True)
            df[column] = pd.to_numeric(df[column], errors='coerce')
        
        if df[column].dtype in ['int64', 'float64']:
            mean = df[column].mean()
            std = df[column].std()
            outlier_mask = np.abs((df[column] - mean) / std) > 3
            outlier_count = outlier_mask.sum()
            
            if outlier_count > 0:
                self.logger.info(f"Found {outlier_count} outliers in {column}")
                df.loc[outlier_mask, column] = np.nan
        
        self.logger.info(f"Cleaned numeric column: {column}")
        return df
    
    def standardize_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], errors='coerce')
                self.logger.info(f"Standardized date column: {column}")
        
        return df
    
    def clean_categorical_column(self, df: pd.DataFrame, column: str, valid_categories: Optional[List[str]] = None) -> pd.DataFrame:
        if column not in df.columns:
            self.logger.warning(f"Column {column} not found in dataframe")
            return df
        
        df[column] = df[column].astype(str).str.title()
        
        if valid_categories:
            valid_categories_title = [cat.title() for cat in valid_categories]
            invalid_mask = ~df[column].isin(valid_categories_title)
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                self.logger.info(f"Found {invalid_count} invalid categories in {column}")
                df.loc[invalid_mask, column] = 'Other'
        
        self.logger.info(f"Cleaned categorical column: {column}")
        return df
    
    def clean_email_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        if column not in df.columns:
            self.logger.warning(f"Column {column} not found in dataframe")
            return df
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        df[column] = df[column].astype(str).str.lower().str.strip()
        
        invalid_mask = ~df[column].str.match(email_pattern, na=False)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            self.logger.info(f"Found {invalid_count} invalid emails in {column}")
            df.loc[invalid_mask, column] = np.nan
        
        self.logger.info(f"Cleaned email column: {column}")
        return df
    
    def clean_dataframe(self, df: pd.DataFrame, cleaning_config: Dict = None) -> pd.DataFrame:
        self.logger.info("Starting data cleaning process")
        
        if cleaning_config is None:
            cleaning_config = {}
        
        # Remove duplicates
        df = self.remove_duplicates(df)
        
        # Handle missing values
        missing_strategy = cleaning_config.get('missing_strategy', {})
        df = self.handle_missing_values(df, missing_strategy)
        
        text_columns = cleaning_config.get('text_columns', [])
        for col in text_columns:
            df = self.clean_text_column(df, col)
        
        numeric_columns = cleaning_config.get('numeric_columns', [])
        for col in numeric_columns:
            df = self.clean_numeric_column(df, col)
        
        date_columns = cleaning_config.get('date_columns', [])
        if date_columns:
            df = self.standardize_dates(df, date_columns)
        
        categorical_columns = cleaning_config.get('categorical_columns', {})
        for col, valid_cats in categorical_columns.items():
            df = self.clean_categorical_column(df, col, valid_cats)
        
        email_columns = cleaning_config.get('email_columns', [])
        for col in email_columns:
            df = self.clean_email_column(df, col)
        
        self.logger.info("Data cleaning process completed")
        return df
