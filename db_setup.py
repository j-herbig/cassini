import os
import pandas as pd
import sqlite3
from tqdm import tqdm

import download_params
import db_params

def get_all_csvs(path: str, path_excluded: str = None) -> list:
    """Gets the paths to all csv files in the given path.

    Args:
        path (str): Path to search csv files in.
        path_excluded (str): Path excluded from search.

    Returns:
        list: Contains paths to all found csv files.
    """
    # find all csv files in a given directory, subdirectories can be excluded (path_axclude)
    csv_files = []
    for root, dirs, files in os.walk(path):
        if not root == path_excluded:
            for file in files:
                if file.split(".")[-1] == "csv":
                    csv_files.append(os.path.join(root, file))

    return csv_files


def get_col_types_for_db(df: pd.DataFrame, primary_key: str = None) -> list:
    """Gets the column types for a sqlite database from dtypes of a given dataframe.

    Args:
        df (pd.DataFrame): Database to derive data types from.
        primary_key (str, optional): Primary key for the database table. Defaults to None.

    Returns:
        list: Database data types for all columns of the given dataframe.
    """
    # Get dtypes of all columns in the given dataframe and translate them to database data types
    col_types = []
    for col in df.columns:
        data_type = ""
        if df[col].dtype == int:
            data_type = "INTEGER"
        elif df[col].dtype == float:
            data_type = "REAL"
        elif df[col].dtype == object:
            data_type = "TEXT"
        # if the given column was specified as primary key, add the corresponding string
        if col == primary_key:
            data_type = data_type + " PRIMARY KEY"
        col_types.append(data_type)
    return col_types


def make_table_command(table_name: str, columns: list, col_types: list) -> str:
    """Creates an SQL command to create a table with the given columns.

    Args:
        table_name (str): Name of the table to create in the database.
        columns (list): Names of the columns of the table.
        col_types (list): Data types of the columns. Must be ordered corresponding to the columns.

    Returns:
        str: SQL command to create the table.
    """
    # make the beginning of the command using the table name
    command = f"CREATE TABLE {table_name} ("
    # add for every column  name and data type 
    for i, col in enumerate(columns):
        command = command + f"{col} {col_types[i]}, "
    # return string without the last comma and whitespace and close the parenthesis
    return command[:-2] + ")"


def make_database(database_path: str, df: pd.DataFrame) -> dict:
    """Makes a database for 'On Time Reporting Carrier On Time Performance' dataset.

    Args:
        database_path (str): Path to store the database.
        df (pd.DataFrame): Dataframe containing data corresponding to 'On Time Reporting Carrier On Time Performance'. Used to derive data types from.

    Returns:
        dict: Table names and corresponding column names of the database.
    """
    # define columns that are used as IDs
    cols_id = [
        "FlightDate",
        "Reporting_Airline", 
        "OriginAirportID", 
        "DestAirportID",
    ]
    # define columns for the time table
    cols_time = [
        "Year", 
        "Quarter", 
        "Month", 
        "DayofMonth", 
        "DayOfWeek", 
        "FlightDate",
    ]
    # define columns for airline table
    cols_airline = [
        "Reporting_Airline",
        "DOT_ID_Reporting_Airline",
        "IATA_CODE_Reporting_Airline",
        # "Tail_Number",
        # "Flight_Number_Reporting_Airline",
    ]
    # define columns of origin airport
    cols_airport_origin = [
        "OriginAirportID",
        "OriginAirportSeqID",
        "OriginCityMarketID",
        "Origin",
        "OriginCityName",
        "OriginState",
        "OriginStateFips",
        "OriginStateName",
        "OriginWac",
    ]
    # define columns of destination airport
    cols_airport_dest = [
        "DestAirportID",
        "DestAirportSeqID",
        "DestCityMarketID",
        "Dest",
        "DestCityName",
        "DestState",
        "DestStateFips",
        "DestStateName",
        "DestWac",
    ]
    # from the definitions before, get columns for the flight table
    cols_flight = [
        col for col in df.columns \
        if (col not in cols_time \
        and col not in cols_airline \
        and col not in cols_airport_origin \
        and col not in cols_airport_dest) \
        or col in cols_id
    ]
    # derive columns for airport table from the destination airport column list
    cols_airport = [
        col[4:] if col != "Dest" else "Airport" for col in cols_airport_dest
    ]

    # add commands to create tables to a list
    make_table_commands = []
    make_table_commands.append(
        make_table_command(
            "time_period", cols_time, get_col_types_for_db(df[cols_time], primary_key="FlightDate",)
        )
    )
    make_table_commands.append(
        make_table_command(
            "airports", cols_airport, get_col_types_for_db(df[cols_airport_dest], primary_key="DestAirportID")
        )
    )
    make_table_commands.append(
        make_table_command(
            "airlines", cols_airline, get_col_types_for_db(df[cols_airline], primary_key="Reporting_Airline",)
        )
    )
    make_table_commands.append(
        make_table_command(
            "flights", cols_flight, get_col_types_for_db(df[cols_flight])
        )
    )

    # open connection and cursor to the database 
    conn = sqlite3.connect(database_path)
    crsr = conn.cursor()

    # make tables using the commands created above
    for cmd in make_table_commands:
        try:
            crsr.execute(cmd)
        except Exception as e:
            print(e)
    
    # close cursor and connection
    crsr.close()
    conn.close()

    # return tables and corresponding column names
    return {"time_period": cols_time, "airports": cols_airport, "airlines": cols_airline, "flights": cols_flight}


def get_uniques(df: pd.DataFrame, column_uniques: str) -> pd.DataFrame:
    """Filters a dataframe by unique values of one column.

    Args:
        df (pd.DataFrame): Dataframe to filter.
        column_uniques (str): Column to look for unique values in.

    Returns:
        pd.DataFrame: Dataframe with unique values of column_uniques apearing only once.
    """
    # add the first row for each unique value in column_uniques to a dataframe
    df_uniques = pd.DataFrame()
    for val in df[column_uniques].unique():
        df_uniques = pd.concat([df_uniques, df[df[column_uniques]==val].iloc[:1,:]])
    return df_uniques


def transfer_look_up_table_to_db(path_file: str, db_connection: sqlite3.Connection):
    """Transfers data of look up tables to a database.

    Args:
        path_file (str): Path to the csv file containing a look up table.
        db_connection (sqlite3.Connection): Connection to a database to transfer data to.
    """
    # use file name without extension as table name
    table_name = path_file.split("/")[-1].split(".")[0]

    # write data to database
    pd.read_csv(path_file).to_sql(table_name, db_connection, if_exists="replace", index=False)


if __name__ == "__main__":
    # get parameters from db_params.py
    db_name = db_params.DB_NAME
    db_path = db_params.DB_PATH
    db_path = os.path.join(db_path, db_name)

    # get csv files from the download folder excluding look up tables
    data_files = get_all_csvs(download_params.DOWNLOAD_PATH, download_params.DOWNLOAD_PATH_LOOKUP)

    # make empty dataframes to append data from the csv files afterwards
    df_time_periods = pd.DataFrame()
    df_airports = pd.DataFrame()
    df_airlines = pd.DataFrame()

    print("transfer data to database...")

    # visualize file processing with a progress bar
    with tqdm(total=len(data_files)) as pbar:
        
        # process the data files found before
        for i, csv_file in enumerate(data_files):

            # read one file into a dataframe
            df = pd.read_csv(csv_file, low_memory=False)

            # drop columns containing 'Unnamed'
            for col in df.columns:
                if "Unnamed" in col:
                    df.drop(col, axis=1, inplace=True)
            
            # in the first loop make the databse and connect to it
            if not i:
                db_tables = make_database(db_path, df)
                conn = sqlite3.connect(db_path)

            # process flights
            # append data of flights to database
            df[db_tables["flights"]].to_sql("flights", conn, if_exists="append", index=False)
            
            # process airports
            # filter for data of destination and origin airports
            dest_airports = df[[f"Dest{col}" if col != "Airport" else "Dest" for col in db_tables["airports"]]]
            origin_airports = df[[f"Origin{col}" if col != "Airport" else "Origin" for col in db_tables["airports"]]]
            # set their column names equal to the column names of the database airport table to simplify appending
            dest_airports.columns = origin_airports.columns = db_tables["airports"]
            # add origin and destination airports to the airports dataframe and only keep unique values
            df_airports = get_uniques(pd.concat([df_airports, dest_airports, origin_airports]), "AirportID")

            # process airlines
            # add only uniques of airlines to the airline dataframe to keep it small 
            airlines = get_uniques(df[db_tables["airlines"]], "Reporting_Airline")
            df_airlines = pd.concat([df_airlines, airlines])

            # process time periods
            # add only uniques of time periods to the time period dataframe to keep it small 
            time_periods = get_uniques(df[db_tables["time_period"]], "FlightDate")
            df_time_periods = pd.concat([df_time_periods, time_periods])
            
            # update progress bar
            pbar.update(1)

    # add the time periods dataframe to the database
    df_time_periods.to_sql("time_period", conn, if_exists="append", index=False)
    # add uniques of airport and airlines dataframe to the database
    get_uniques(df_airports, "AirportID").to_sql("airports", conn, if_exists="append", index=False)
    get_uniques(df_airlines, "Reporting_Airline").to_sql("airlines", conn, if_exists="append", index=False)

    print("transfer look up tables to database...")
    # add look up tables to the database
    for look_up_table in tqdm(get_all_csvs(download_params.DOWNLOAD_PATH_LOOKUP)):
        transfer_look_up_table_to_db(look_up_table, conn)

    # close database connection
    conn.close()
