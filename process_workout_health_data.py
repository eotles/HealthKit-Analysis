#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process Workout Health Data
==============================
:File: process_workout_health_data
:Description: This script finds Apple HealthKit data realted to workouts by filtering through the health CSV and workout route directory, then saving the filtered data to a specified output file.
:Version: 0.0.1
:Created: 2023-11-06
:Author: Erkin Otles
:Dependencies: directory with workout routes, a health CSV produced by apple_health_xml_convert.py
:License: MIT License
"""

import argparse
from datetime import datetime, timedelta, timezone
import os
import pandas as pd
import pytz
import sys
import xml.etree.ElementTree as ET

def gpx_time_string_to_datetime(time_string):
    return datetime.fromisoformat(time_string.rstrip("Z")).replace(tzinfo=pytz.UTC)

    
def extract_times_from_gpx(gpx_fp):
    tree = ET.parse(gpx_fp)
    root = tree.getroot()

    # Define namespaces
    ns = {
        'gpx': 'http://www.topografix.com/GPX/1/1'
    }

    all_timestamps = []

    # Extract all timestamps from trkpts
    for trkseg in root.findall(".//gpx:trk/gpx:trkseg", ns):
        for trkpt in trkseg.findall("gpx:trkpt", ns):
            time_element = trkpt.find("gpx:time", ns)
            if time_element is not None:
                all_timestamps.append(time_element.text)

    # Get the first and last timestamps
    if all_timestamps:
        first_time = gpx_time_string_to_datetime(all_timestamps[0])
        last_time = gpx_time_string_to_datetime(all_timestamps[-1])
    else:
        first_time, last_time = None, None

    return first_time, last_time

def process_workout_routes_dir(workout_routes_dir):
    results = {}

    for filename in os.listdir(workout_routes_dir):
        if filename.endswith(".gpx"):
            full_path = os.path.join(workout_routes_dir, filename)
            first_time, last_time = extract_times_from_gpx(full_path)
            results[filename] = {"startDate": first_time, "endDate": last_time}

    return results


def _parse_date_with_offset(date_str):
    # Split the string into datetime and offset parts
    dt_str, offset_str = date_str[:-6], date_str[-5:]
    
    # Create a timezone-aware datetime object
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    
    # Calculate the offset in minutes
    offset_minutes = int(offset_str[1:3]) * 60 + int(offset_str[3:5])
    if offset_str[0] == '-':
        offset_minutes = -offset_minutes

    # Apply the offset to create a timezone-aware datetime object
    tz_aware_dt = dt.replace(tzinfo=timezone(timedelta(minutes=offset_minutes)))

    return tz_aware_dt
 
 
def _getMidDates(workout_health_df):
    type_wh_dfs = []
    for type in workout_health_df['type'].unique():
        type_wh_df = workout_health_df[workout_health_df['type']==type].copy()
        type_wh_df = type_wh_df.sort_values(by='startDate')
    
        if len(type_wh_df)==1:
            type_wh_df['midStartDate'] = type_wh_df['startDate']
            type_wh_df['midEndDate'] = type_wh_df['endDate']
    
        else:
            midDates = ((type_wh_df['startDate'].shift(-1) - type_wh_df['startDate'])/2 + type_wh_df['startDate'])
            dateList = [type_wh_df['startDate'].iloc[0]] + midDates.to_list()[:-1] + [type_wh_df['startDate'].iloc[-1]]
            type_wh_df['midStartDate'] = dateList[:-1]
            type_wh_df['midEndDate'] = dateList[1:]
    
        type_wh_dfs.append(type_wh_df)
    return pd.concat(type_wh_dfs)


def filter_health_export(workout_routes_dir, health_csv_fp):
    #read workouts
    print("Reading workouts...", end="")
    sys.stdout.flush()
    
    workout_datetimes = process_workout_routes_dir(workout_routes_dir)
    print("done!")

    #read health CSV
    print("Reading health CSV file...", end="")
    sys.stdout.flush()
    
    health_df = pd.read_csv(health_csv_fp, low_memory=False)
    print("done!")
    
    #preparing health data
    print("Preparing health data...", end="")
    sys.stdout.flush()
    
    # Values to numeric - drop rows with other types
    health_df['value'] = pd.to_numeric(health_df['value'], errors='coerce')
    health_df = health_df[~health_df['value'].isna()]

    # Convert time strings to UTC datetime
    time_cns = ['startDate', 'endDate']
    for cn in time_cns:
        health_df[cn] = health_df[cn].apply(_parse_date_with_offset)
    print("done!")


    #extracting workout related rows from health
    print("Extracting workout related health data...", end="")
    sys.stdout.flush()
    
    res = []
    for workout_date, workout_times in workout_datetimes.items():
        workout_health_df = health_df[(health_df['startDate']>= workout_times['startDate']) & (health_df['endDate']<= workout_times['endDate'])]
        types = workout_health_df['type'].unique()
        
        if "VO2Max" in types:
            workout_health_df = workout_health_df.copy()
            workout_health_df.loc[:, 'workout_date'] = workout_date
            
            #get midDates
            workout_health_df = _getMidDates(workout_health_df)
            
            res.append(workout_health_df)
    
    print("done!")
    return pd.concat(res)

# Function to process the workout health data
def process_workout_health_data(workout_routes_dir, health_csv_fp, workout_health_df_fp):
    workout_health_df = filter_health_export(workout_routes_dir, health_csv_fp)
    workout_health_df.to_csv(workout_health_df_fp, index=False)
    print(f"Data processed and exported to {workout_health_df_fp}")

# Main function to parse arguments and call the processing function
def main():
    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Process Apple Health Export Data.")
    
    # Add arguments to the parser
    parser.add_argument("workout_routes_dir", type=str, help="Directory containing workout route data")
    parser.add_argument("health_csv_fp", type=str, help="File path to the health CSV file")
    parser.add_argument("--output", "-o", type=str, default="../data/apple_workout_health_export.csv",
                        help="File path for the output CSV (optional)")

    # Parse the arguments
    args = parser.parse_args()

    # Call the processing function with the provided arguments
    process_workout_health_data(args.workout_routes_dir, args.health_csv_fp, args.output)

if __name__ == "__main__":
    main()
