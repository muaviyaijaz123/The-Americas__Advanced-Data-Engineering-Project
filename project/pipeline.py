import os
import shutil
import platform
import subprocess
import zipfile
import pandas as pd
import sqlite3

#GLOBAL VARIABLES

script_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(script_directory)

#-------------------------------- Set Kaggle API ------------------------#
def setKaggleAPI():
    kaggle_dir = os.path.expanduser("~/.kaggle")
    os.makedirs(kaggle_dir, exist_ok=True)


    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory where the script is located
    kaggle_source_file_path = os.path.join(script_directory, 'kaggle.json')  # Adjust if kaggle.json is elsewhere
    #kaggle_source_file_path = 'kaggle.json'  # Use the full path if kaggle.json is not in the same directory as this script
    kaggle_destination_folder_path = os.path.join(kaggle_dir, 'kaggle.json')
    print(kaggle_source_file_path)

    if not os.path.exists(kaggle_source_file_path):
        raise FileNotFoundError(f"The kaggle file {kaggle_source_file_path} does not exist! Verify its path again.")

    shutil.copy(kaggle_source_file_path, kaggle_destination_folder_path)

    if platform.system() != 'Windows':
        os.chmod(kaggle_destination_folder_path, 0o600)

#-------------------------------- Set Kaggle API ------------------------#

# ------------------------------ ETL Pipeline ----------------------------#    

# -----------------EXTRACT-----------------#

def data_sets_extraction(dataset):
    # script_directory = os.path.dirname(os.path.abspath(__file__))
    # parent_directory = os.path.dirname(script_directory)
    

    data_directory_path = os.path.join(parent_directory,"data")
    print(data_directory_path)
    subprocess.run(["kaggle", "datasets", "download", "-d", dataset,"-p",data_directory_path], check=True)
        
    zip_file_path = os.path.join(data_directory_path, dataset.split('/')[1]+ ".zip")

    if not os.path.exists(data_directory_path):
        os.makedirs(data_directory_path)

    if os.path.exists(zip_file_path):
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(data_directory_path)
                print("Dataset extracted - Success!")
                    
                csv_file = zip_ref.namelist()
                df = pd.read_csv(data_directory_path+"/"+csv_file[0])

                os.remove(zip_file_path)
                print(f"Zip file being removed: {zip_file_path}")
                return df
        except zipfile.BadZipFile as badzip:
            print(f"File is not compatible with ZIP format. Error while unzipping {zip_file_path}: {badzip}")
        except Exception as e:
            print(f"Other unknown error during extracting {zip_file_path}: {e}")

    else:
        print("The zip file does not exist.")
        
# -----------------EXTRACT-----------------#

# -----------------TRANSFORM-----------------#

def fill_missing_values(df, columns, strategy = "mean"):
    
    for col in columns:
        if col in df.columns:
            value_to_fill = 0
            if(strategy == "mean"):
                value_to_fill = df[col].mean()
            elif(strategy == "mode"):
                value_to_fill = df[col].mode()[0]
            elif(strategy == "median"):
                value_to_fill = df[col].median()
            else:
                print("Unknown strategy paramter passed")
                return df

            df.fillna({col: value_to_fill}, inplace=True)
    return df

def fill_missing_values_excluding_columns(df, exclude_columns = [], strategy = "mean"):

    columns_to_fill = [col for col in df.columns if col not in exclude_columns]
    
    for col in columns_to_fill:
        if col in df.columns:
            value_to_fill = 0
            if(strategy == "mean"):
                value_to_fill = df[col].mean()
            elif(strategy == "mode"):
                value_to_fill = df[col].mode()[0]
            elif(strategy == "median"):
                value_to_fill = df[col].median()
            else:
                print("Unknown strategy paramter passed")
                return df

            df.fillna({col: value_to_fill}, inplace=True)
    
    return df

def drop_duplicates(df, columns_subset = None):
    return df.drop_duplicates(subset = columns_subset)

def merge_data_sets(wages_data_transformed, employment_data_transformed):
    return pd.merge(wages_data_transformed,employment_data_transformed) 

def transform_wages_data_set(wages_data):
    
    #1. Remove unnecessary columns
    wages_data_columns_to_keep = ['year'] + [col for col in wages_data.columns if 'white' in col or 'black' in col]
    wages_data = wages_data[wages_data_columns_to_keep]

    wages_years_to_remove = [1973,1974,1975,1976,1977,1978]
    wages_data = wages_data[~wages_data["year"].isin(wages_years_to_remove)]

    #2. Renaming columns
    wages_data_mapper = {
    'white_less_than_hs': 'White_Less_HS_Hourly_Wage',
    'white_high_school': 'White_HS_Hourly_Wage',
    'white_some_college': 'White_Some_College_Hourly_Wage',
    'white_bachelors_degree': 'White_Bachelors_Hourly_Wage',
    'white_advanced_degree': 'White_Advanced_Hourly_Wage',
    'white_men_less_than_hs': 'White_Men_Less_HS_Hourly_Wage',
    'white_men_high_school': 'White_Men_HS_Hourly_Wage',
    'white_men_some_college': 'White_Men_Some_College_Hourly_Wage',
    'white_men_bachelors_degree': 'White_Men_Bachelors_Hourly_Wage',
    'white_men_advanced_degree': 'White_Men_Advanced_Hourly_Wage',
    'white_women_less_than_hs': 'White_Women_Less_HS_Hourly_Wage',
    'white_women_high_school': 'White_Women_HS_Hourly_Wage',
    'white_women_some_college': 'White_Women_Some_College_Hourly_Wage',
    'white_women_bachelors_degree': 'White_Women_Bachelors_Hourly_Wage',
    'white_women_advanced_degree': 'White_Women_Advanced_Hourly_Wage',
    'black_less_than_hs': 'Black_Less_HS_Hourly_Wage',
    'black_high_school': 'Black_HS_Hourly_Wage',
    'black_some_college': 'Black_Some_College_Hourly_Wage',
    'black_bachelors_degree': 'Black_Bachelors_Hourly_Wage',
    'black_advanced_degree': 'Black_Advanced_Hourly_Wage',
    'black_men_less_than_hs': 'Black_Men_Less_HS_Hourly_Wage',
    'black_men_high_school': 'Black_Men_HS_Hourly_Wage',
    'black_men_some_college': 'Black_Men_Some_College_Hourly_Wage',
    'black_men_bachelors_degree': 'Black_Men_Bachelors_Hourly_Wage',
    'black_men_advanced_degree': 'Black_Men_Advanced_Hourly_Wage',
    'black_women_less_than_hs': 'Black_Women_Less_HS_Hourly_Wage',
    'black_women_high_school': 'Black_Women_HS_Hourly_Wage',
    'black_women_some_college': 'Black_Women_Some_College_Hourly_Wage',
    'black_women_bachelors_degree': 'Black_Women_Bachelors_Degree_Hourly_Wage',
    'black_women_advanced_degree': 'Black_Women_Advanced_Degree_Hourly_Wage' 
    }
    wages_data = wages_data.rename(columns = wages_data_mapper)
    
    #3. Remove duplicates 
    wages_data = drop_duplicates(wages_data)

    #4. fill na values with imputation strategy defined by user, default = "mean"
    wages_data = fill_missing_values_excluding_columns(wages_data, ["year"], "mean")


    #5. Adding new columns - White and Black people avg hourly wage for both men and women combined

    white_columns = ['White_Less_HS_Hourly_Wage', 'White_HS_Hourly_Wage', 'White_Some_College_Hourly_Wage',
                 'White_Bachelors_Hourly_Wage', 'White_Advanced_Hourly_Wage']
    wages_data['White_People_Average_Hourly_Wage'] = wages_data[white_columns].mean(axis=1)

    black_columns = ['Black_Less_HS_Hourly_Wage', 'Black_HS_Hourly_Wage', 'Black_Some_College_Hourly_Wage',
                 'Black_Bachelors_Hourly_Wage', 'Black_Advanced_Hourly_Wage']
    wages_data['Black_People_Average_Hourly_Wage'] = wages_data[black_columns].mean(axis=1)

    wages_data['Black_People_Average_Hourly_Wage'] = wages_data['Black_People_Average_Hourly_Wage'].round(2)
    wages_data['White_People_Average_Hourly_Wage'] = wages_data['White_People_Average_Hourly_Wage'].round(2)
    
    return wages_data


def transform_employment_data_set(employment_data):
    #1. Remove unnecessary columns
    employment_data_to_keep = ['year'] + ['total_population'] + [col for col in employment_data.columns if 'white' in col or 'black' in col]
    employment_data = employment_data[employment_data_to_keep]

    #2. Renaming columns
    employment_data_mapper = {
    'black': 'Black_Employment_Ratio_All_Ages',
    'black_16-24': 'Black_Employment_Ratio_Age_16_24',
    'black_25-54': 'Black_Employment_Ratio_Age_25_54',
    'black_55-64': 'Black_Employment_Ratio_Age_55_64',
    'black_65+': 'Black_Employment_Ratio_Age_65_Plus',
    'black_less_than_hs': 'Black_Employment_Ratio_Less_Than_High_School',
    'black_high_school': 'Black_Employment_Ratio_High_School',
    'black_some_college': 'Black_Employment_Ratio_Some_College',
    'black_bachelors_degree': 'Black_Employment_Ratio_Bachelors_Degree',
    'black_advanced_degree': 'Black_Employment_Ratio_Advanced_Degree',
    'black_women': 'Black_Women_Employment_Ratio_All_Ages',
    'black_women_16-24': 'Black_Women_Employment_Ratio_Age_16_24',
    'black_women_25-54': 'Black_Women_Employment_Ratio_Age_25_54',
    'black_women_55-64': 'Black_Women_Employment_Ratio_Age_55_64',
    'black_women_65+': 'Black_Women_Employment_Ratio_Age_65_Plus',
    'black_women_less_than_hs': 'Black_Women_Employment_Ratio_Less_Than_High_School',
    'black_women_high_school': 'Black_Women_Employment_Ratio_High_School',
    'black_women_some_college': 'Black_Women_Employment_Ratio_Some_College',
    'black_women_bachelors_degree': 'Black_Women_Employment_Ratio_Bachelors_Degree',
    'black_women_advanced_degree': 'Black_Women_Employment_Ratio_Advanced_Degree',
    'black_men': 'Black_Men_Employment_Ratio_All_Ages',
    'black_men_16-24': 'Black_Men_Employment_Ratio_Age_16_24',
    'black_men_25-54': 'Black_Men_Employment_Ratio_Age_25_54',
    'black_men_55-64': 'Black_Men_Employment_Ratio_Age_55_64',
    'black_men_65+': 'Black_Men_Employment_Ratio_Age_65_Plus',
    'black_men_less_than_hs': 'Black_Men_Employment_Ratio_Less_Than_High_School',
    'black_men_high_school': 'Black_Men_Employment_Ratio_High_School',
    'black_men_some_college': 'Black_Men_Employment_Ratio_Some_College',
    'black_men_bachelors_degree': 'Black_Men_Employment_Ratio_Bachelors_Degree',
    'black_men_advanced_degree': 'Black_Men_Employment_Ratio_Advanced_Degree',
    
    'white': 'White_Employment_Ratio_All_Ages',
    'white_16-24': 'White_Employment_Ratio_Age_16_24',
    'white_25-54': 'White_Employment_Ratio_Age_25_54',
    'white_55-64': 'White_Employment_Ratio_Age_55_64',
    'white_65+': 'White_Employment_Ratio_Age_65_Plus',
    'white_less_than_hs': 'White_Employment_Ratio_Less_Than_High_School',
    'white_high_school': 'White_Employment_Ratio_High_School',
    'white_some_college': 'White_Employment_Ratio_Some_College',
    'white_bachelors_degree': 'White_Employment_Ratio_Bachelors_Degree',
    'white_advanced_degree': 'White_Employment_Ratio_Advanced_Degree',
    'white_women': 'White_Women_Employment_Ratio_All_Ages',
    'white_women_16-24': 'White_Women_Employment_Ratio_Age_16_24',
    'white_women_25-54': 'White_Women_Employment_Ratio_Age_25_54',
    'white_women_55-64': 'White_Women_Employment_Ratio_Age_55_64',
    'white_women_65+': 'White_Women_Employment_Ratio_Age_65_Plus',
    'white_women_less_than_hs': 'White_Women_Employment_Ratio_Less_Than_High_School',
    'white_women_high_school': 'White_Women_Employment_Ratio_High_School',
    'white_women_some_college': 'White_Women_Employment_Ratio_Some_College',
    'white_women_bachelors_degree': 'White_Women_Employment_Ratio_Bachelors_Degree',
    'white_women_advanced_degree': 'White_Women_Employment_Ratio_Advanced_Degree',
    'white_men': 'White_Men_Employment_Ratio_All_Ages',
    'white_men_16-24': 'White_Men_Employment_Ratio_Age_16_24',
    'white_men_25-54': 'White_Men_Employment_Ratio_Age_25_54',
    'white_men_55-64': 'White_Men_Employment_Ratio_Age_55_64',
    'white_men_65+': 'White_Men_Employment_Ratio_Age_65_Plus',
    'white_men_less_than_hs': 'White_Men_Employment_Ratio_Less_Than_High_School',
    'white_men_high_school': 'White_Men_Employment_Ratio_High_School',
    'white_men_some_college': 'White_Men_Employment_Ratio_Some_College',
    'white_men_bachelors_degree': 'White_Men_Employment_Ratio_Bachelors_Degree',
    'white_men_advanced_degree': 'White_Men_Employment_Ratio_Advanced_Degree'
    }
    employment_data = employment_data.rename(columns = employment_data_mapper)

    #3. Remove duplicates 
    employment_data = drop_duplicates(employment_data)

    #4. fill na values with imputation strategy defined by user, default = "mean"
    employment_data = fill_missing_values_excluding_columns(employment_data, ["year","total_population"], "mean")

    return employment_data

def merged_data_set_transformation(df):
    #6. changing dtype of year fields and other to 'int16' and 'float32' for efficient memory storage
    df["year"] = df["year"].astype("int16")
    
        
    #7. Reorder columns, moving total population to second position
    cols = list(df.columns)
    cols.remove("total_population")  
    cols.insert(1, "total_population") 
    df = df[cols]

    #8. sort dataframe by year
    df.sort_values(by='year', ascending=False, inplace=True)

    return  df
# -----------------TRANSFORM-----------------#

# -----------------LOAD-----------------#
def load_datasets(df):
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # parent_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(parent_directory, 'data')
    db_path = os.path.join(data_dir, 'wages_and_employment_data.db')
    
    conn = sqlite3.connect(db_path)
    df.to_sql('wages_and_employment_ratio_by_education', conn, if_exists='replace', index=False)
    conn.close()
# -----------------LOAD-----------------#

def main():
    # Please sign into Kaggle -> Go to Settings
    # Create API token -> place kaggle.json file into project directory
    # Run the script
    dataset_names = [
    "asaniczka/wages-by-education-in-the-usa-1973-2022",
    "asaniczka/employment-to-population-ratio-for-usa-1979-2023",
    ]

    setKaggleAPI()
    
    wage_by_education_dataset = data_sets_extraction(dataset_names[0])
    employment_to_population_dataset = data_sets_extraction(dataset_names[1])

    transformed_wages_data_set = transform_wages_data_set(wage_by_education_dataset)
    transformed_employment_data_set = transform_employment_data_set(employment_to_population_dataset)
    
    merged_data_set = merge_data_sets(transformed_wages_data_set, transformed_employment_data_set )
    
    merged_data_set = merge_data_sets(transformed_wages_data_set, transformed_employment_data_set )
    final_transformed_data_set = merged_data_set_transformation(merged_data_set)

    load_datasets(final_transformed_data_set)

if __name__ == "__main__":
    main()
