import os
from pathlib import Path


class ETLConfig:
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / "data"
        self.raw_data_dir = self.data_dir / "raw"
        self.processed_data_dir = self.data_dir / "processed"
        self.logs_dir = self.project_root / "logs"
        self.output_dir = self.project_root / "output"
        
        self.tech_jobs_file = self.raw_data_dir / "tech_jobs.csv"
        self.tech_investment_file = self.raw_data_dir / "tech_investment.csv"
        self.tech_skill_profiles_file = self.raw_data_dir / "skill_profiles.csv"
        
        self.warehouse_file = self.processed_data_dir / "TechWarehouse.csv"
        self.quality_report_file = self.output_dir / "reporte_calidad.txt"
        self.metrics_file = self.output_dir / "metricas_etl.json"
        
        self.chunk_size = 10000
        self.max_null_percentage = 50
        self.date_format = "%Y-%m-%d"
        self.encoding = "utf-8"
        
        self.min_records_threshold = 100
        self.duplicate_threshold = 5
        
        self._create_directories()
    
    def _create_directories(self):
        for directory in [self.logs_dir, self.output_dir, self.processed_data_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_database_config(self):
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'techskills'),
            'username': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD', '')
        }
