import os, sys
import pandas as pd
import numpy as np
from ..logger import logging
from ..exception import CustomMessage
from .data_transformation import DataTransformation
from sklearn.model_selection import train_test_split
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    train_data_path:str = os.path.join("artifacts/data", "train.csv")
    test_data_path:str = os.path.join("artifacts/data", "test.csv")
    raw_data_path:str = os.path.join("artifacts/data", "raw.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Data Ingestion Process Started")
        try:
            data = pd.read_csv("C:/Users/vamsh/OneDrive/Desktop/Movie Recomendation System/data/final_data.csv")
            logging.info("Data reading Completed.")

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            data.to_csv(self.ingestion_config.raw_data_path, index=False)
            
            logging.info("splitting data")
            train_set, test_set = train_test_split(data, test_size=0.3, random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            test_set.to_csv(self.ingestion_config.test_data_path, index=False, header=True)

            logging.info("Data Ingestion completed")

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )

        except Exception as e:
            logging.error("An error occured during the data ingestion process")
            raise CustomMessage(e, sys)
        
if __name__ == "__main__":
    obj = DataIngestion()
    train_data_path, test_data_path = obj.initiate_data_ingestion()

    data_transformation = DataTransformation()
