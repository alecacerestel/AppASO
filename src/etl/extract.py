"""
Data extraction module.
Generates mock data for the ETL pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List


class DataExtractor:
    """
    Handles data extraction. Currently generates mock data.
    """
    
    def __init__(self):
        """
        Initialize data extractor.
        """
        self.sources = ["API", "Web", "Database"]
        self.categories = ["Sales", "Marketing", "Operations", "Finance", "HR"]
    
    def extract_data(self, num_records: int = 100) -> pd.DataFrame:
        """
        Extract data for processing.
        Currently generates mock data for testing purposes.
        
        Args:
            num_records: Number of records to generate
            
        Returns:
            DataFrame with mock data
        """
        np.random.seed(int(datetime.now().timestamp()) % (2**32))
        
        data = {
            "id": range(1, num_records + 1),
            "fecha": [datetime.now().strftime("%Y-%m-%d")] * num_records,
            "fuente": np.random.choice(self.sources, num_records),
            "valor": np.round(np.random.uniform(10.0, 1000.0, num_records), 2),
            "categoria": np.random.choice(self.categories, num_records)
        }
        
        df = pd.DataFrame(data)
        
        # Introduce some null values randomly for transformation testing
        null_indices = np.random.choice(df.index, size=int(num_records * 0.05), replace=False)
        df.loc[null_indices, "valor"] = np.nan
        
        return df
