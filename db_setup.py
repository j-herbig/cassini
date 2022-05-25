import os
import pandas as pd
import sqlite3
from tqdm import tqdm

import download_params
import db_params

def get_all_csvs(path: str) -> list:
    """Gets the paths to all csv files in the given path.

    Args:
        path (str): Path to search csv files in.

    Returns:
        list: Contains paths to all found csv files.
    """
    csv_files = []
    for root, dirs, files in os.walk(path):
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
    col_types = []
    for col in df.columns:
        data_type = ""
        if df[col].dtype == int:
            data_type = "INTEGER"
        elif df[col].dtype == float:
            data_type = "REAL"
        elif df[col].dtype == object:
            data_type = "TEXT"
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
    command = f"CREATE TABLE {table_name} ("
    for i, col in enumerate(columns):
        command = command + f"{col} {col_types[i]}, "
    return command[:-2] + ")"


def make_database(database_path: str, df: pd.DataFrame) -> dict:
    """Makes a database for 'On Time Reporting Carrier On Time Performance' dataset.

    Args:
        database_path (str): Path to store the database.
        df (pd.DataFrame): Dataframe containing data corresponding to 'On Time Reporting Carrier On Time Performance'. Used to derive data types from.

    Returns:
        dict: Table names and corresponding column names of the database.
    """
    cols_id = [
        "FlightDate",
        "Reporting_Airline", 
        "OriginAirportID", 
        "DestAirportID",
    ]
    cols_time = [
        "Year", 
        "Quarter", 
        "Month", 
        "DayofMonth", 
        "DayOfWeek", 
        "FlightDate",
    ]
    cols_airline = [
        "Reporting_Airline",
        "DOT_ID_Reporting_Airline",
        "IATA_CODE_Reporting_Airline",
        # "Tail_Number",
        # "Flight_Number_Reporting_Airline",
    ]
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
    cols_flight = [
        col for col in df.columns \
        if (col not in cols_time \
        and col not in cols_airline \
        and col not in cols_airport_origin \
        and col not in cols_airport_dest) \
        or col in cols_id
    ]
    cols_airport = [
        col[4:] if col != "Dest" else "Airport" for col in cols_airport_dest
    ]

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

    conn = sqlite3.connect(database_path)
    crsr = conn.cursor()

    for cmd in make_table_commands:
        try:
            crsr.execute(cmd)
        except Exception as e:
            print(e)
    crsr.close()
    conn.close()

    return {"time_period": cols_time, "airports": cols_airport, "airlines": cols_airline, "flights": cols_flight}


def get_uniques(df: pd.DataFrame, column_uniques: str) -> pd.DataFrame:
    """Filters a dataframe by unique values of one column.

    Args:
        df (pd.DataFrame): Dataframe to filter.
        column_uniques (str): Column to look for unique values in.

    Returns:
        pd.DataFrame: Dataframe with unique values of column_uniques apearing only once.
    """
    # uniques = df[column_uniques].unique()
    df_uniques = pd.DataFrame()
    for val in df[column_uniques].unique():
        df_uniques = pd.concat([df_uniques, df[df[column_uniques]==val].iloc[:1,:]])
    return df_uniques


if __name__ == "__main__":
    db_name = db_params.DB_NAME
    db_path = db_params.DB_PATH
    db_path = os.path.join(db_path, db_name)
    csv_files = get_all_csvs(download_params.DOWNLOAD_PATH)
    df_time_periods = pd.DataFrame()
    df_airports = pd.DataFrame()
    df_airlines = pd.DataFrame()

    with tqdm(total=len(csv_files)) as pbar:

        for i, csv_file in enumerate(csv_files):
            df = pd.read_csv(csv_file, low_memory=False)
            for col in df.columns:
                if "Unnamed" in col:
                    df.drop(col, axis=1, inplace=True)
            if not i:
                db_tables = make_database(db_path, df)
                conn = sqlite3.connect(db_path)
            # add_data_to_db("on_time_performance", db_path, df, col_groups)
            df[db_tables["flights"]].to_sql("flights", conn, if_exists="append", index=False)
            
            # airports
            dest_airports = df[[f"Dest{col}" if col != "Airport" else "Dest" for col in db_tables["airports"]]]
            origin_airports = df[[f"Origin{col}" if col != "Airport" else "Origin" for col in db_tables["airports"]]]
            dest_airports.columns = origin_airports.columns = db_tables["airports"]
            airports = pd.concat([dest_airports, origin_airports])
            airports = get_uniques(airports, "AirportID")
            df_airports = pd.concat([df_airports, airports])

            # airlines
            airlines = get_uniques(df[db_tables["airlines"]], "Reporting_Airline")
            df_airlines = pd.concat([df_airlines, airlines])

            # time periods
            time_periods = get_uniques(df[db_tables["time_period"]], "FlightDate")
            df_time_periods = pd.concat([df_time_periods, time_periods])
            pbar.update(1)


    df_time_periods.to_sql("time_period", conn, if_exists="append", index=False)
    get_uniques(df_airports, "AirportID").to_sql("airports", conn, if_exists="append", index=False)
    get_uniques(df_airlines, "Reporting_Airline").to_sql("airlines", conn, if_exists="append", index=False)

    conn.close()
