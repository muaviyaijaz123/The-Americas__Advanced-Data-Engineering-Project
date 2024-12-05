import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sqlite3
from pipeline import (
    setKaggleAPI,
    data_sets_extraction,
    fill_missing_values_excluding_columns,
    transform_wages_data_set,
    transform_employment_data_set,
    merge_data_sets,
    merged_data_set_transformation,
    load_datasets,
    main
)

class TestPipeline(unittest.TestCase):

    def setUp(self):

        self.dataset_names = [
            "asaniczka/wages-by-education-in-the-usa-1973-2022",
            "asaniczka/employment-to-population-ratio-for-usa-1979-2023",
        ]
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.parent_directory = os.path.dirname(self.script_directory)
        self.data_directory = os.path.join(self.parent_directory, "data")

        self.db_path = os.path.join(self.data_directory, 'wages_and_employment_data.db')

    def tearDown(self):            
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    # Unit Test 1: Test Kaggle API Setup
    def test_1_set_kaggle_api(self):
        print("-------------------Test Case: Kaggle Api Setup (check if kaggle.json exists)-------------\n")
        setKaggleAPI()
        kaggle_dir = os.path.expanduser("~/.kaggle")
        kaggle_file = os.path.join(kaggle_dir, "kaggle.json")
        self.assertTrue(os.path.exists(kaggle_file), "Kaggle.json file not found in the expected location")
        print("Test Case Status: PASSED -> Kaggle API setup is correct, and kaggle.json file exists in the user home directory\n")

    # Unit Test 2: Handle Missing Vales in Data
    def test_2_fill_missing_values_excluding_columns(self):
        
        print("-------------------Test Case: Handle Missing Values Correctly-------------\n")
        # sample_data
        data = {
            'year': [2000, 2001, 2002, 2003],
            'column1': [1, None, 3, None],
            'column2': [None, 2, 3, 4],
            'column3': [5, 6, None, None]
        }
        df = pd.DataFrame(data)
        
        # expected data after strategy applied
        expected_data = {
            'year': [2000, 2001, 2002, 2003], 
            'column1': [1, 2.0, 3, 2.0], 
            'column2': [3.0, 2, 3, 4],  
            'column3': [5, 6, 5.5, 5.5]  
        }
        expected_df = pd.DataFrame(expected_data)
        
        # Exclude 'year' column from imputation
        result_df = fill_missing_values_excluding_columns(df, exclude_columns=['year'], strategy='mean')

        # Assert that the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)
        print("Test Case Status: PASSED -> Missing values in the function are handled correctly.\n")


    # Unit Test 3: Testing Merge Datasets
    def test_3_merge_datasets(self):
        print("-------------------Test Case: Handle Merge Datasets-------------\n")
        
        wages_data = pd.DataFrame({
            "year": [1980, 1981],
            "White_Less_HS_Hourly_Wage": [17.3, 18.9],
            "White_Bachelors_Hourly_Wage": [18.65, 19.65],
        })
        employment_data = pd.DataFrame({
            "year": [1980, 1981],
            "Black_Employment_Ratio_All_Ages": [52.8, 32.5],
        })
        
        # Merge
        merged_data = merge_data_sets(wages_data, employment_data)

        # Expected shape: (2,4)
        expected_shape = (2, 4)
        self.assertEqual(merged_data.shape, expected_shape, f"Expected merged DataFrame shape {expected_shape}, but got {merged_data.shape}")

        # Validate all expected columns are present
        expected_columns = [
            "year",
            "White_Less_HS_Hourly_Wage",
            "White_Bachelors_Hourly_Wage",
            "Black_Employment_Ratio_All_Ages",
        ]
        for col in expected_columns:
            self.assertIn(col, merged_data.columns, f"Missing expected column: {col}")

        # Validate row alignment
        self.assertEqual(merged_data.loc[0, "year"], 1980, "Mismatch in merged data row alignment for year 1980")
        self.assertEqual(merged_data.loc[1, "year"], 1981, "Mismatch in merged data row alignment for year 1981")

        # Validate specific cell values
        self.assertEqual(merged_data.loc[0, "White_Less_HS_Hourly_Wage"], 17.3, "Mismatch in data for year 1980, column 'White_Less_HS_Hourly_Wage'")
        self.assertEqual(merged_data.loc[1, "Black_Employment_Ratio_All_Ages"], 32.5, "Mismatch in data for year 1981, column 'Black_Employment_Ratio_All_Ages'")

        print("Test Case Status: PASSED -> Datasets merged successfully.\n")

    # Unit Test 4: Test SQL Load Function
    @patch("pandas.DataFrame.to_sql")
    @patch("sqlite3.connect")
    def test_6_load_datasets(self, mock_connect, mock_to_sql):
        print("-------------------Test Case: Check SQL Load Functionality-------------\n")
        
        # Mocking database connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        data = pd.DataFrame({
            "year": [1980, 1981],
            "White_Less_HS_Hourly_Wage": [17.3, 18.9],
            "Black_Employment_Ratio_All_Ages": [52.8, 32.5]
        })

        load_datasets(data)

        # Assert that the database connection was established
        mock_connect.assert_called_once()

        # Assert that to_sql was called with the correct parameters
        mock_to_sql.assert_called_once_with(
            'wages_and_employment_ratio_by_education', 
            mock_conn, 
            if_exists='replace', 
            index=False
        )

        # Assert that the connection was closed
        mock_conn.close.assert_called_once()
        print("Test Passed: DataFrame successfully mocked and passed to to_sql.\n")

    @patch("pipeline.os.path.exists")
    @patch("pipeline.os.makedirs")
    @patch("pipeline.subprocess.run")
    @patch("pipeline.zipfile.ZipFile")
    @patch("pipeline.pd.read_csv")
    @patch("pipeline.os.remove")
    def test_4_data_extraction(self,mock_remove, mock_read_csv, mock_zipfile, mock_run, mock_makedirs, mock_exists):
        print("-------------------Test Case: Check Data Set Extraction Functionality-------------\n")
        # Setup mocks
        mock_exists.side_effect = lambda path: True if "wages-by-education-in-the-usa-1973-2022.zip" in path else False  # Simulate ZIP file exists
        mock_run.return_value = MagicMock()  # Mock subprocess.run for Kaggle download
        
        # Mock ZIP file extraction
        mock_zip_instance = MagicMock()
        mock_zip_instance.namelist.return_value = ["wages_by_education.csv"]
        mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

        # Mock pandas.read_csv to return a test DataFrame
        mock_read_csv.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        # Run the function
        dataset = self.dataset_names[0]
        df = data_sets_extraction(dataset)

        # Assertions
        mock_run.assert_called_once_with(
            ["kaggle", "datasets", "download", "-d", dataset, "-p", self.data_directory],
            check=True
        )  # Verify subprocess was called with correct command

        zip_file_path = os.path.join(self.data_directory,"wages-by-education-in-the-usa-1973-2022.zip")
        csv_file_path = os.path.join(self.data_directory,"wages_by_education.csv")
        
        mock_zipfile.assert_called_once_with(zip_file_path, "r") 
        mock_remove.assert_called_once_with(zip_file_path)
        mock_read_csv.assert_called_once_with(csv_file_path)  # Check if CSV was read
        print("Test Case Status: PASSED. DataFrame extraction functionlity working .\n")


    # Integration Test 1: Transform DataSet
    def test_5_transform_wages_data_set_with_original_data(self):
        print("-------------------Test Case: Check Data Set Transform Functionality-------------\n")
        setKaggleAPI()
        
        original_data = data_sets_extraction(self.dataset_names[0])

        # Transform the dataset using the transformation function
        transformed_data = transform_wages_data_set(original_data)

        # Validating resuting shape here
        expected_shape = (44, 33)  # Replace this with the expected shape for your data
        self.assertEqual(transformed_data.shape, expected_shape, f"Expected shape {expected_shape}, but got {transformed_data.shape}")
        
        print("Test Passed: Transformed original data has the expected shape and structure.\n")

    # System Test: Pipeline Execution and Checking Output here
    def test_6_pipeline_execution(self):
        print("-------------------SYSTEM LEVEL TEST: Pipeline Check-------------\n")

        # Main Pipeline function executed
        main()

        # Check for database file existence
        self.assertTrue(os.path.exists(self.db_path), "ETL Pipeline did not create the expected output file")

        # Connect to the database and fetch data
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # verifying one table existence here
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        self.assertTrue(len(tables) > 0, "ETL Pipeline did not create any table....Please verify your pipeline")

        # Fetch the data from the created table
        table_name = tables[0][0]  # Assuming there's only one table
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)

        conn.close()

        # Checking expected shape
        expected_shape = (44, 94)
        self.assertEqual(df.shape, expected_shape, f"Expected DataFrame shape {expected_shape}, but got {df.shape}")

        print("-------------------SYSTEM LEVEL TEST STATUS: Pipeline Run successfully-------------\n")

if __name__ == "__main__":
    unittest.main()
