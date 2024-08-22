import os, sys
import pandas as pd
import numpy as np
import ast
from ..logger import logging
from ..exception import CustomMessage
from dataclasses import dataclass

@dataclass
class DataTransformationConfig:
    preprocessor_obj_file_path = os.path.join("artifacts/data_transformation", "preprocessor.pkl")

class DataTransformation:
    def __init__(self):
        self.data_transformation_config = DataTransformationConfig()

    def merge_data(self, df1, df2):
        try:
            df = df1.merge(df2, on='title')
            return df
        except Exception as e:
            logging.info("Cannot merge data")
            raise CustomMessage(e, sys)
        
    def remove_columns(self, df):
        try:
            columns = ['movie_id', 'title', 'genres', 'keywords', 'title', 'overview', 'cast', 'crew']
            df = df[columns]
            return df
        except Exception as e:
            logging.info("Removing columns has failed")
            raise CustomMessage(e, sys)
        
    def remove_null_and_duplicates(self, df):
        try:
            df  = df.dropna(inplace=True)
            return df   
        except Exception as e:
            logging.info("cannot remove duplicates")
            raise CustomMessage(e, sys)
        
    def convert_dictionary_to_list(self, obj):
        List = []
        for i in ast.literal_eval(obj):
           List.append(i['name'])
        return List
    
    
    def convert(self, df):
        try:
         cols = ['genres', 'keywords', 'cast']
         for i in cols:
             df[i] = df[i].apply(self.convert_dictionary_to_list)
             return df
        except Exception as e:
            raise CustomMessage(e, sys)
        
    def FetchDirector(self, obj):
        List = []
        for i in ast.literal_eval(obj):
          if i['job'] == 'Director':
            List.append(i['name'])
            break
        return List
    
    def convert2(self, df):
        try:
            col = 'crew'
            if col not in df.columns:
                raise ValueError("The col is missing in the dataframe")
            df[col] = df[col].apply(self.FetchDirector)
            return df
        except Exception as e:
            raise CustomMessage(e, sys)
        
    def split(self, text):
        return text.split()
    
    def splitting(self, df):
        try:
          df['overview'] = df['overview'].apply(self.split)
          return df
        except Exception as e:
            raise CustomMessage(e, sys)
    
    def replace(self, lst):
        return [item.replace(" ", "") for item in lst]
    
    def remove_spaces_from_list(self, df):
       try:
        cols = ['genres', 'keywords', 'crew', 'cast']
        for i in cols:
            df[i] = df[i].apply(self.replace)
            return df
       except Exception as e:
           raise CustomMessage(e, sys)
       
    def new_columns(self, df):
        try:
            df['tags'] = df['overview'] + df['keywords'] + df['cast'] + df['crew']
            return df


        except Exception as e:
            raise CustomMessage(e, sys)
        
    def new_dataframe(self, df):
        try:
            cols = ['movie_id', 'title', 'tags']
            new_df = df[cols]
            return new_df
        except Exception as e:
            raise CustomMessage(e, sys) 
        
    def join_tags(self, tags_list):
       return " ".join(tags_list)
    
    def final_transform(self, new_df):
        try:
            new_df['tags'] = new_df['tags'].apply(self.join_tags)
            return new_df
        except Exception as e:
            raise CustomMessage(e, sys)
        

    def initiate_data_transformation(self, df1, df2):
        try:
            df1 = pd.read_csv(df1)
            df2 = pd.read_csv(df2)
            # Step 1: Merge train and test data on 'title' column
            merged_data = self.merge_data(df1, df2)
        
            # Step 2: Remove unwanted columns
            merged_data = self.remove_columns(merged_data)
        
            # Step 3: Remove null values and duplicates
            cleaned_data = self.remove_null_and_duplicates(merged_data)
        
            # Step 4: Convert dictionary-like columns to list (genres, keywords, cast)
            list_converted_data = self.convert(cleaned_data)
        
            # Step 5: Extract Director information from 'crew'
            director_data = self.convert2(list_converted_data)
        
            # Step 6: Split 'overview' text into words
            split_data = self.splitting(director_data)
        
            # Step 7: Remove spaces from elements in 'genres', 'keywords', 'crew', and 'cast'
            no_space_data = self.remove_spaces_from_list(split_data)
        
            # Step 8: Create a 'tags' column by combining 'overview', 'keywords', 'cast', and 'crew'
            tags_data = self.new_columns(no_space_data)
        
            # Step 9: Select only 'movie_id', 'title', and 'tags' columns to create the final DataFrame
            final_df = self.new_dataframe(tags_data)
        
            # Step 10: Join tags into a single string
            transformed_df = self.final_transform(final_df)

            return transformed_df


        except Exception as e:
            raise CustomMessage(e, sys)
    

    
        
        

    



        
    
       
    


    
