import pandas as pd
from core import QueryRunner, Expectation, ExpectationResponse


class CheckDatabaseHasExpectedTables(Expectation):
    """Receives a set with table names and checks if all of
    the tables can be found in the database. It returns True if all found and
    False if at least one is not found. It doesn't verify if additional tables
    are present in the database side. 
    """     
    def check(self, expected_tables_set: set, fail_if_found_more_than_expected: bool = True):             
        
        expectation_response = ExpectationResponse(expectation_input=locals())
        
        sql_query ="SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        found_tables_set = self.query_runner.run_query(sql_query, return_only_first_col_as_set=True)
                
        if fail_if_found_more_than_expected:
            expectation_response.result = (expected_tables_set == found_tables_set)
        else:
            expectation_response.result = (expected_tables_set.issubset(found_tables_set))

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: difference between expected tables and found tables on the db see details"
            expectation_response.details = {"expected_tables":expected_tables_set, 
                                            "found_tables":found_tables_set}

        return expectation_response

class CheckTableHasExpectedColumns(Expectation):
    """Receives a table name and a set with column names and 
    checks if all of columns can be found in the table. It returns True if all 
    found and False if at least one is not found. It doesn't verify if additional
    columns are present in the table. 
    """ 
    def check(self, table_name: str, expected_columns_set: set, fail_if_found_more_than_expected: bool = True):
        
        expectation_response = ExpectationResponse(expectation_input=locals())        
        
        sql_query =f"SELECT name FROM pragma_table_info('{table_name}');"
        found_columns_set = self.query_runner.run_query(sql_query,return_only_first_col_as_set=True)
        
        if fail_if_found_more_than_expected:
            expectation_response.result = (expected_columns_set == found_columns_set)            
        else:
            expectation_response.result = (expected_columns_set.issubset(found_columns_set))

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: difference between expected columns and found columnson table '{table_name}' see details"
            expectation_response.details = {"table":table_name, 
                                            "expected_columns":expected_columns_set, 
                                            "found_columns":found_columns_set}

        return expectation_response


class CheckTableHasRowCountInExpectedRange(Expectation):
    """Receives a table name and a min and max for row count. It
    returns True if the row count is within the range and False otherwise, inclusive
    of min and max. 
    """
    def check(self, table_name: str, min_expected_row_count: int, max_expected_row_count: int):
        
        expectation_response = ExpectationResponse(expectation_input=locals())
        
        sql_query =f"SELECT COUNT(*) AS row_count FROM {table_name};"
        counted_rows = self.query_runner.run_query(sql_query)['row_count'][0]
        
        expectation_response.result = (min_expected_row_count <= counted_rows <= max_expected_row_count)

        if expectation_response.result:            
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: row count not in the expected range for table '{table_name}' see details"
            expectation_response.details = {"table":table_name, 
                                            "counted_rows":min_expected_row_count, 
                                            "min_expected":min_expected_row_count, 
                                            "max_expected":max_expected_row_count}

        return expectation_response
        
class CheckRowCountGroupedByField(Expectation):
    """Receives a table name, a field name and a dictionary with row count 
    ranges for how many rows have the values for that field. For example: 
    with a table Person, a field Mother_First_Name and a dictionary: 
        {"mary":(2,5), "ellen": (1,3)} 
    it will test if the number of rows in the table Person that have the 
    field Mother_First_Name = "mary" is between 2 and 5 (inclusive of 2 and 5) 
    and if the number of rows that have Mother_First_Name = "ellen" is between 
    1 and 3. If the row counts for all fields are inside the ranges it will 
    return True, if at least one of the counts is not inside the range it will
    return False
    """
    def check(self, table_name: str, field_name: str, count_ranges_per_value: dict):        
        
        expectation_response = ExpectationResponse(expectation_input=locals())
        
        df_expected_counts_by_value = pd.DataFrame(count_ranges_per_value)
        sql_query =f"SELECT {field_name} AS lookup_value,COUNT(*) AS rows_found FROM {table_name} GROUP BY {field_name};"
        counted_rows_by_grouped_value = self.query_runner.run_query(sql_query)  
        expected_versus_found_df = df_expected_counts_by_value.merge(counted_rows_by_grouped_value, on="lookup_value", how='left')        
        found_not_within_range = expected_versus_found_df.loc[
            (expected_versus_found_df['rows_found'] >= expected_versus_found_df['max_row_count']) 
            | (expected_versus_found_df['rows_found'] <= expected_versus_found_df['min_row_count'])
        ]

        expectation_response.result = (len(found_not_within_range) == 0)

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: table '{table_name}': one or more counts per lookup_value not in expected range see for more info see details"
            expectation_response.details = found_not_within_range.to_dict(orient='records')
            
        return expectation_response

class CheckFieldValuesWithinExpectedSet(Expectation):
    """Receives a table name, a field and a set expected values and 
    checks if values found for that field are within the expected set
    It returns True if all are in the expected set and False if at 
    least one is not. 
    By default is single-sided: returns False only if found a value that
    is not in the expected set, but doens't return False if an non-expected
    value is present. 
    If the flag fail_if_not_found_entire_expected_set is set to True it will
    also return False in cases where a value of the expected set was not 
    present in the table.    
    """ 
    def check(self, table_name: str, field:str, expected_values_set: set, fail_if_not_found_entire_expected_set: bool = False):
        
        expectation_response = ExpectationResponse(expectation_input=locals())        
        
        sql_query =f"SELECT {field} FROM {table_name} GROUP BY 1;"
        found_values_set = self.query_runner.run_query(sql_query,return_only_first_col_as_set=True)
        
        if fail_if_not_found_entire_expected_set:
            expectation_response.result = (expected_values_set == found_values_set)            
        else:
            expectation_response.result = (found_values_set.issubset(expected_values_set))

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: values for field '{field}' on table '{table_name}' do not fit expected set criteria, see details"
            expectation_response.details = {"table":table_name, 
                                            "expected_values":expected_values_set, 
                                            "found_values":found_values_set}

        return expectation_response

class CheckUniquenessForFieldOrSetOfFields(Expectation):
    """Receives a table name, a field (or a set of fields) and checks
    the table doesn't have duplicity for that field (or set of fields).
    Returns True if in the table there are not 2 rows with identical values
    for the field (or set of fields) and False otherwise.    
    """ 
    def check(self, table_name: str, fields:list):        
        
        expectation_response = ExpectationResponse(expectation_input=locals())        

        str_fields = ",".join(fields)        
        sql_query =f"SELECT {str_fields},COUNT(*) AS duplicates_count FROM {table_name} GROUP BY {str_fields} HAVING COUNT(*)>1;"        
        found_duplicity = self.query_runner.run_query(sql_query)

        expectation_response.result = (len(found_duplicity) == 0)

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: duplicate values for the combined fields '{fields}' on table '{table_name}', see details"
            expectation_response.details = {"duplicates_found": found_duplicity.to_dict(orient='records')}
        
        return expectation_response

class CheckGeoShapesAreValid(Expectation):
    """Receives a table name, a shape field and an shape ref field (or set of)
    checks that the shapes are valid. Returns True if all are valid. Returns
    False if shapes are invalid and in the details returns ref for the shapes
    that were invalid (is_valid=0) or if shape parsing failed unknown(is_valid=-1).
    """ 
    def check(self, table_name: str, shape_field: str, ref_fields:list):        
        
        expectation_response = ExpectationResponse(expectation_input=locals())       

        str_ref_fields = ",".join(ref_fields)     
        sql_query =f"""
            SELECT {str_ref_fields}, ST_IsValid(ST_GeomFromText({shape_field})) AS is_valid 
            FROM {table_name}
            WHERE ST_IsValid(ST_GeomFromText({shape_field})) IN (0,-1);"""
        invalid_shapes = self.query_runner.run_query(sql_query)

        expectation_response.result = (len(invalid_shapes) == 0)

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: invalid shapes found in field '{shape_field}' on table '{table_name}', see details"
            expectation_response.details = {"invalid_shapes": invalid_shapes.to_dict(orient='records')}
        
        return expectation_response

class CheckJsonKeysAreWithinExpectedSetOfKeys(Expectation):
    """Receives a table name, a field name (of a field that has a JSON text 
    stored in it) and a set of keys. It checks if the keys found in the JSON 
    are within the expected set of expected keys. 
    One sided: will not check if all expected keys are found, only if all 
    found are within the expected
    """ 
    def check(self, table_name: str, field_name: str, expected_keys_set: set, ref_fields:list):
        
        expectation_response = ExpectationResponse(expectation_input=locals())        

        str_ref_fields = ",".join(ref_fields)  
        
        prep_key_set = [f"'$.{s}'" for s in expected_keys_set]
        str_expected_keys_set = ",".join(prep_key_set)

        print(str_expected_keys_set)

        sql_query =f"""
            SELECT {str_ref_fields},json_remove({field_name}, {str_expected_keys_set}) AS non_expected_keys 
            FROM {table_name} 
            WHERE 
                json_remove({field_name}, {str_expected_keys_set}) !=""" + "'{}'" +f" OR json_remove({field_name}, {str_expected_keys_set}) IS NULL;"
        
        non_expected_keys = self.query_runner.run_query(sql_query)

        expectation_response.result = (len(non_expected_keys) == 0)
        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: found non-expected json keys in the field '{field_name}' on table '{table_name}', see details"
            expectation_response.details = {"records_with_non_expected_keys": non_expected_keys.to_dict(orient='records') }

        return expectation_response
    
class CheckJsonValuesForKeyAreWithinExpectedSet(Expectation):
    """Receives:
        - table name, 
        - a field (with a JSON text in it), 
        - a json key (that for which the value will be looked)
        - one or more ref columns in a list,
        - a set expected values.
    It returns True if both conditions are met:
        - the key was found for all rows
        - the value stored for the key was within expected set
    If any is not met it returns False.
    One-sided: will not check if all expected values are found, only if all 
    found values are within the expected
    """ 
    def check(self, table_name: str, field:str, json_key: str, expected_values_set: set, ref_fields:list):

        expectation_response = ExpectationResponse(expectation_input=locals())        
        
        str_ref_fields = ",".join(ref_fields)  
        str_expected_values_set = "','".join(expected_values_set)

        sql_query =f"""
            SELECT {str_ref_fields},json_extract({field}, '$.{json_key}') AS value_found_for_key 
            FROM {table_name} 
            WHERE 
                (json_extract({field}, '$.{json_key}') NOT IN ('{str_expected_values_set}'))
                OR (json_extract({field}, '$.{json_key}')) IS NULL;"""

        non_expected_values = self.query_runner.run_query(sql_query)
        
        expectation_response.result = (len(non_expected_values) == 0)

        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: found non-expected values for key '{json_key}' in field '{field}' on table '{table_name}', see details"
            expectation_response.details = {"non_expected_values": non_expected_values.to_dict(orient='records') }

        return expectation_response

class CheckValueInFieldIsWithinExpectedRange(Expectation):
    """Receives a table name, a field name checks the values found in the field
    are within the expected range
    """ 
    def check(self, table_name: str, field_name: str, min_expected_value: int, max_expected_value: int, ref_fields:list):
        
        expectation_response = ExpectationResponse(expectation_input=locals())        

        str_ref_fields = ",".join(ref_fields)

        sql_query=f"""SELECT {str_ref_fields},{field_name} 
                      FROM {table_name} 
                      WHERE {field_name} < {min_expected_value} OR {field_name} > {max_expected_value}"""
        
        records_with_value_out_of_range = self.query_runner.run_query(sql_query)

        print(len(records_with_value_out_of_range))

        expectation_response.result = (len(records_with_value_out_of_range) == 0)
        if expectation_response.result:
            expectation_response.msg = "Success: data quality as expected"
            expectation_response.details = None
        else:
            expectation_response.msg = f"Fail: found values out of the expected range for field '{field_name}' on table '{table_name}', see details"
            expectation_response.details = {"records_with_value_out_of_range": records_with_value_out_of_range.to_dict(orient='records') }

        return expectation_response