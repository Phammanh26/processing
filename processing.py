
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import *
from datetime import date

def num_month(colx):
    today = date.today()
    num_year = today.year - year(colx)
    num_month = today.month - month(colx)
    range_month = num_year * 12 + num_month
    return range_month

def extract_text(text, list_check, null_value =""):
    if text != None:
        text = text.lower().strip()
        for subtext in list_check:
            subtext = subtext.lower().strip()
            if subtext in text:
                return subtext
    return null_value
class DatetimeExtractor():
    
    def __init__(self, input_col, output_col='date_extractor_default', drop_input_col = True, null_value = -1):
        self.input_col = input_col
        self.output_col = output_col
        self.null_value = null_value
        self.drop_input_col = drop_input_col

    def check_input_type(self, schema):
        field = schema[self.input_col]
        if (field.dataType != TimestampType()):
            raise Exception('YearExtractor input type %s did not match input type DateType' % field.dataType)
    
    def _month_transform(self, df):
        self.check_input_type(df.schema)
        df = df.withColumn(self.input_col, num_month(col(self.input_col)))
        
        ## drop input column after extract information from it
        if self.drop_input_col == True:
            df = df.drop(self.input_col)
        return df
    
    def _age_transform(self, df):
        self.check_input_type(df.schema)
        df = df.withColumn(self.output_col,floor(datediff(current_date(),col(self.input_col))/365.25))
        df = df.na.fill(value=self.null_value,subset=[self.output_col])
        
        ## drop input column after extract information from it
        if self.drop_input_col == True:
            df = df.drop(self.input_col)
        
        return df

class AddressExtractor():
    def __init__(self, input_col, output_col='default', drop_input_col = True, null_value = "", vobab_address_dict = None):
        
        ## input_col is column is extracted addess
        self.input_col = input_col
        
        ## output_col is new column constain addess extracted
        if output_col == 'default':
            self.output_col = input_col + "_default"
        else:
            self.output_col = output_col
        
        self.null_value = null_value
        self.drop_input_col = drop_input_col
        self.cities = []
        self.provinces = []
        
        if vobab_address_dict != None and  type(vobab_address_dict) == dict:
            self.cities = list(vobab_address_dict.keys())
            for address in vobab_address_dict:
                self.provinces.extend(vobab_address_dict[address])
            
    def extract_city(self, text):
        return extract_text(text, self.cities)
    
    def extract_province(self, text):
        return extract_text(text, self.provinces)
    
    def extract_address(self, text):

        address_city_province = []
        city = self.extract_city(text)
        province = self.extract_province(text)

        if city != self.null_value:
            address_city_province.append(city)
        if province != self.null_value:
            address_city_province.append(province)
        
        address_city_province_string  = ", ".join(address_city_province)

        return address_city_province_string
    
    def _address_transform(self, df):

        self.check_input_type(df.schema)

        udf_extract_address= udf(lambda x:self.extract_address(x),StringType())
        df_e = df.withColumn(self.output_col,udf_extract_address(col(self.input_col)))

        ## drop input column after extract information from it
        if self.drop_input_col == True:
            df = df.drop(self.input_col)

        return df_e
