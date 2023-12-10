CREATE OR REPLACE PROCEDURE LOAD_EXCEL_FILES_TO_TABLE_SP(file_path string, worksheet_name string, target_table string)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd

def main(session, file_path, worksheet_name, target_table):
    with SnowflakeFile.open(file_path,'rb') as f:
       workbook = load_workbook(f)
       sheet = workbook.get_sheet_by_name(worksheet_name)
       data = sheet.values

       #First line in file is header, get it
       columns = next(data)[0:]
       #Create df 
       df = pd.DataFrame(data, columns = columns)

       df2 = session.create_dataframe(df)
       df2.write.mode("overwrite").save_as_table(target_table)
    
    return True
$$;

--CALL LOAD_EXCEL_FILES_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@DATA_RAW_STAGE , 'intro/location.xlsx'),'location','LOCATION');
--CALL LOAD_EXCEL_FILES_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@DATA_RAW_STAGE , 'intro/order_detail.xlsx'),'order_detail','ORDER_DETAILS');



