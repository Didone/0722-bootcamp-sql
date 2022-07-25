"""Test module"""
from os import environ
import shutil

environ['LOG_LEVEL']='DEBUG'
environ['CSVMS_DEFAULT_DB']="mock"
environ['CSVMS_FILE_DIR']="tests/data"
shutil.rmtree(environ['CSVMS_FILE_DIR'], ignore_errors=True)
