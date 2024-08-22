from .logger import logging
from .exception import CustomMessage
import os, sys
import pickle

def save_object(file_path, obj):
    try:
        directory_path = os.path.dirname(file_path)
        os.makedirs(directory_path, exist_ok=True)

        with open(file_path, 'wb') as file_obj:
            pickle.dump(obj, file_obj)
    except Exception as e:
        raise CustomMessage(e, sys)