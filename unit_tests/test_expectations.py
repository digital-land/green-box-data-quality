from xmlrpc.client import TRANSPORT_ERROR
import pytest
import pandas as pd
import spatialite
from expectations import *

# Shared testing resources
tested_dataset = "/src/sharing_area/green-box-data-quality/unit_tests/testing_dataset/lb_single_res.sqlite3"
query_runner = QueryRunner(tested_dataset)

def test_check_database_has_expected_tables_Success():
    "Test with tables that all exist in the database, with fail if found more than expected (fme) True"
    expected_table_set = {"column_field", "dataset_resource", "entity", "fact", "fact_resource", "issue", "old_entity"}
    check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner)    
    response = check_for_expected_tables.check(expected_table_set,fail_if_found_more_than_expected = True)

    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_check_database_has_expected_tables_Success_with_fme_false():
    "Test with tables that all exist in the database, with fail if found more than expected False"
    expected_table_set = {"column_field", "dataset_resource", "entity", "fact", "fact_resource", "issue", "old_entity"}
    check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner)    
    response = check_for_expected_tables.check(expected_table_set,fail_if_found_more_than_expected = False)

    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_check_database_has_expected_tables_Fail():
    "Test with one table that won't be found in the database, with fail if found more than expected True"
    expected_table_set = {"not_found_table","column_field", "dataset_resource", "entity", "fact", "fact_resource", 
                        "issue", "old_entity"}    
    check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner)    
    response = check_for_expected_tables.check(expected_table_set,fail_if_found_more_than_expected = True)

    assert response.result == False

def test_check_database_has_expected_tables_Fail_with_fme_false():
    "Test with one table that won't be found in the database, with fail if found more than expected False"
    expected_table_set = {"not_found_table","column_field", "dataset_resource", "entity", "fact", "fact_resource", 
                        "issue", "old_entity"}    
    check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner)    
    response = check_for_expected_tables.check(expected_table_set,fail_if_found_more_than_expected = False)

    assert response.result == False


def test_check_database_has_expected_tables_find_more_than_expected_fail():
    "All expected will be found, but will find one non-expected with fail if found more than expected True, should fail"
    expected_table_set = { "dataset_resource", "entity", "fact", "fact_resource", 
                        "issue", "old_entity"}    
    check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner) 
    response = check_for_expected_tables.check(expected_table_set,fail_if_found_more_than_expected = True)

    assert response.result == False


def test_check_table_has_expected_columns_Success():
    "Test with all columns found in the table, return should be True"    
    check_for_expected_columns = CheckTableHasExpectedColumns(query_runner = query_runner)
    table_name = "entity"
    expected_column_set = {'typology', 'start_date', 'reference', 
                            'geometry', 'entity', 'end_date', 'json', 
                            'dataset', 'geojson', 'point', 'entry_date', 
                            'name', 'organisation_entity', 'prefix'}     
    response =  check_for_expected_columns.check(
        table_name=table_name,expected_columns_set= expected_column_set)
    
    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_check_table_has_expected_columns_Fail(): 
    "Test with a column that won't be found in the table, return should be False"
    check_for_expected_columns = CheckTableHasExpectedColumns(query_runner = query_runner)
    table_name = "entity"
    expected_column_set = {'not_found_column', 'typology','start_date', 'reference', 
                            'geometry', 'entity', 'end_date', 'json', 
                            'dataset', 'geojson', 'point', 'entry_date', 
                            'name', 'organisation_entity', 'prefix'}        
    response =  check_for_expected_columns.check(
        table_name=table_name,expected_columns_set= expected_column_set)

    assert response.result == False

def test_check_table_has_expected_columns_fail_if_find_more_than_expected_Fail(): 
    "All columns will be found, but there is a found column that was not expected 'typology', should return False"
    check_for_expected_columns = CheckTableHasExpectedColumns(query_runner = query_runner)
    table_name = "entity"
    fail_if_found_more_than_expected=True
    expected_column_set = { 'start_date', 'reference', 
                            'geometry', 'entity', 'end_date', 'json', 
                            'dataset', 'geojson', 'point', 'entry_date', 
                            'name', 'organisation_entity', 'prefix'}        
    response = check_for_expected_columns.check(
        table_name=table_name,expected_columns_set= expected_column_set,fail_if_found_more_than_expected=fail_if_found_more_than_expected)
    
    assert response.result == False

def test_check_table_has_expected_columns_success_if_find_more_than_expected_Success(): 
    """All columns will be found, but there is a found column that was not expected 'typology',
    but flag to fail for this is off, should return True"""
    check_for_expected_columns = CheckTableHasExpectedColumns(query_runner = query_runner)
    table_name = "entity"
    fail_if_found_more_than_expected=False
    expected_column_set = { 'start_date', 'reference', 
                            'geometry', 'entity', 'end_date', 'json', 
                            'dataset', 'geojson', 'point', 'entry_date', 
                            'name', 'organisation_entity', 'prefix'}        
    response = check_for_expected_columns.check(
        table_name=table_name,expected_columns_set= expected_column_set, fail_if_found_more_than_expected=fail_if_found_more_than_expected)

    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None


def test_row_count_of_table_is_in_expected_range_Success(): 
    "Test with count in the expected range"
    check_for_expected_columns = CheckTableHasRowCountInExpectedRange(query_runner = query_runner)
    table_name = "entity"
    min_expected_row_count = 400
    max_expected_row_count = 500
    response = check_for_expected_columns.check(
                                    table_name=table_name,
                                    min_expected_row_count=min_expected_row_count,
                                    max_expected_row_count = max_expected_row_count)

    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_row_count_of_table_is_in_expected_range_Fail(): 
    "Test with count not in the expected range"
    check_for_expected_columns = CheckTableHasRowCountInExpectedRange(query_runner = query_runner)
    table_name = "entity"
    min_expected_row_count = 200
    max_expected_row_count = 300
    response = check_for_expected_columns.check(
                                    table_name=table_name,
                                    min_expected_row_count=min_expected_row_count,
                                    max_expected_row_count = max_expected_row_count)
    print(response)
    assert response.result == False
    assert response.msg == "Fail: row count not in the expected range for table 'entity' see details"
    assert response.details == {'table': 'entity', 'counted_rows': 200, 'min_expected': 200, 'max_expected': 300}

def test_row_count_grouped_by_field_Success2():
    "Test with one of the count per value not within expected ranges"
    table_name = "fact"
    field_name = "entity"
    dict_value_and_count_ranges = [
       {"lookup_value":42114488,"min_row_count":8,"max_row_count":10},
       {"lookup_value":42114489,"min_row_count":8,"max_row_count":10},
       {"lookup_value":42114490,"min_row_count":8,"max_row_count":10}]

    check_row_counts_by_field = CheckRowCountGroupedByField(query_runner=query_runner)
    response = check_row_counts_by_field.check(table_name=table_name, 
                                                field_name=field_name ,  
                                                count_ranges_per_value=dict_value_and_count_ranges)
    
    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_row_count_grouped_by_field_Fail():
    "Test with one of the count per value not within expected range (42114490)"
    table_name = "fact"
    field_name = "entity"
    dict_value_and_count_ranges = [
       {"lookup_value":42114488,"min_row_count":8,"max_row_count":10},
       {"lookup_value":42114489,"min_row_count":8,"max_row_count":10},
       {"lookup_value":42114490,"min_row_count":6,"max_row_count":8}]    

    check_row_counts_by_field = CheckRowCountGroupedByField(query_runner=query_runner)
    response = check_row_counts_by_field.check(table_name=table_name, 
                                                field_name=field_name ,  
                                                count_ranges_per_value=dict_value_and_count_ranges)

    assert response.result == False
    assert response.msg == "Fail: table 'fact': one or more counts per lookup_value not in expected range see for more info see details"
    assert response.details == [{'lookup_value': 42114490, 'min_row_count': 6, 'max_row_count': 8, 'rows_found': 9}]

def test_check_field_values_within_expected_set_of_values_No_unexpected_value():   
    "Returns True as all values are within the expected set (but not full expected set is found)"
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = False
    expected_values_set = {"not_present_value",
                            "entry-date", 
                            "organisation", 
                            "reference", 
                            "listed-building-grade", 
                            "geometry", 
                            "prefix", 
                            "notes", 
                            "name", 
                            "description"}

    check_field_values_within_expected_set = CheckFieldValuesWithinExpectedSet(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, field_name,expected_values_set,fail_if_not_found_entire_expected_set)
    
    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None    

def test_check_field_values_within_expected_set_of_values_One_unexpected_value():   
    "Returns False because it finds 'entry-date' as a value that was not in the the expected set"
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = False
    expected_values_set = {"not_present_value",                                                        
                            "organisation", 
                            "reference", 
                            "listed-building-grade", 
                            "geometry", 
                            "prefix", 
                            "notes", 
                            "name", 
                            "description"}
   
       
    check_field_values_within_expected_set = CheckFieldValuesWithinExpectedSet(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, field_name,expected_values_set,fail_if_not_found_entire_expected_set)
   
    assert response.result == False
    assert response.msg == "Fail: values for field 'field' on table 'fact' do not fit expected set criteria, see details"
    assert response.details == {
        'table': 'fact', 
        'expected_values': {
            'not_present_value', 
            'name', 
            'description', 
            'organisation', 
            'reference', 
            'listed-building-grade', 
            'prefix', 
            'notes', 
            'geometry'},
        'found_values': {
            'name', 
            'entry-date', 
            'description', 
            'organisation', 
            'reference', 
            'listed-building-grade', 
            'prefix', 
            'notes', 
            'geometry'}}

def test_check_field_values_within_expected_set_of_values_Not_found_entire_expected_set_found():   
    """Returns False because even though all values are within expected set the flag
    fail_if_not_found_entire_expected_set is set to True and the 'not_present_value' is not 
    found in the table field.
    """
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = True
    expected_values_set = {"not_present_value",
                            "entry-date", 
                            "organisation", 
                            "reference", 
                            "listed-building-grade", 
                            "geometry", 
                            "prefix", 
                            "notes", 
                            "name", 
                            "description"}   
       
    check_field_values_within_expected_set = CheckFieldValuesWithinExpectedSet(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, field_name,expected_values_set,fail_if_not_found_entire_expected_set)
    
    print(response)

    
    assert response.result == False
    assert response.msg == "Fail: values for field 'field' on table 'fact' do not fit expected set criteria, see details"
    assert response.details == {
        'table': 'fact', 
        'expected_values': {
            'not_present_value', 
            'description', 
            'listed-building-grade',             
            'reference', 
            'geometry', 
            'notes', 
            'entry-date', 
            'organisation', 
            'prefix', 
            'name'}, 
        'found_values': {
            'description', 
            'listed-building-grade', 
            'reference', 
            'geometry', 
            'notes', 
            'entry-date', 
            'organisation', 
            'prefix', 
            'name'}}


def test_check_field_values_within_expected_set_of_values_Entire_expected_set_found():   
    """Returns True because all values are within expected and all are found and the flag
    fail_if_not_found_entire_expected_set is set to True.
    """
    table_name = "fact"
    field_name = "field"
    fail_if_not_found_entire_expected_set = True
    expected_values_set = {
                            "entry-date", 
                            "organisation", 
                            "reference", 
                            "listed-building-grade", 
                            "geometry", 
                            "prefix", 
                            "notes", 
                            "name", 
                            "description"}   
       
    check_field_values_within_expected_set = CheckFieldValuesWithinExpectedSet(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, field_name,expected_values_set,fail_if_not_found_entire_expected_set)
           
    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None

def test_check_uniqueness_field_set_of_fields_True():   
    """Test uniqueness with combination field that is unique.
    Should return True for uniqueness test
    """
    table_name = "fact"
    fields = ["fact"]      
       
    check_field_values_within_expected_set = CheckUniquenessForFieldOrSetOfFields(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, fields)

              
    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None
    
def test_check_uniqueness_field_set_of_fields_False():   
    """Test uniqueness with combination of 2 fields that have multiple values.
    Should return False for uniqueness test
    """
    table_name = "fact"
    fields = ["field","entry_date"]      
       
    check_field_values_within_expected_set = CheckUniquenessForFieldOrSetOfFields(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, fields)

              
    assert response.result == False
    assert response.msg == "Fail: duplicate values for the combined fields '['field', 'entry_date']' on table 'fact', see details"
    assert response.details == {
        'duplicates_found': [
            {'field': 'description', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'entry-date', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'geometry', 'entry_date': '2022-07-31', 'duplicates_count': 489}, 
            {'field': 'listed-building-grade', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'name', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'notes', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'organisation', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'prefix', 'entry_date': '2022-07-31', 'duplicates_count': 465}, 
            {'field': 'reference', 'entry_date': '2022-07-31', 'duplicates_count': 465}]
        }

def test_check_geo_shapes_are_valid_True():   
    """ Tests with five valid geo shapes, should return True.
    """
    tested_dataset = "/src/sharing_area/green-box-data-quality/unit_tests/testing_dataset/five_valid_multipolygons.sqlite3"    
    query_runner = QueryRunner(tested_dataset)
    
    table_name = "five_valid_multipolygons"
    shape_field ="geometry"
    ref_fields = ["entity"]      
    
    check_field_values_within_expected_set = CheckGeoShapesAreValid(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, shape_field, ref_fields)

    assert response.result == True
    assert response.msg == "Success: data quality as expected"
    assert response.details == None    

def test_check_geo_shapes_are_valid_False():   
    """ Tests with one geo shape that is invalid in a table with 5.
    """
    tested_dataset = "/src/sharing_area/green-box-data-quality/unit_tests/testing_dataset/one_invalid_among_five.sqlite3"
    query_runner = QueryRunner(tested_dataset)
    
    table_name = "one_invalid_among_five"
    shape_field ="geometry"
    ref_fields = ["entity"]      
    
    check_field_values_within_expected_set = CheckGeoShapesAreValid(query_runner=query_runner)
    response = check_field_values_within_expected_set.check(table_name, shape_field, ref_fields)

    print(response)

    assert response.result == False    
    assert response.msg == "Fail: invalid shapes found in field 'geometry' on table 'one_invalid_among_five', see details"
    assert response.details == {'invalid_shapes': [{'entity': 303443, 'is_valid': 0}]}
                  