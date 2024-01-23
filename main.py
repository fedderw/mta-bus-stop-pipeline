import argparse
import calendar
import datetime as dt
import re
import subprocess
from datetime import datetime
from pathlib import Path

import boto3
import geopandas as gpd
import pandas as pd
import requests
from prefect import task
from prefect.flows import flow

# Dictionary mapping short color codes to their corresponding CityLink route names
color_to_citylink = {
    "BL": "CityLink Blue",
    "BR": "CityLink Brown",
    "CityLink BLUE": "CityLink Blue",
    "CityLink NAVY": "CityLink Navy",
    "CityLink ORANGE": "CityLink Orange",
    "CityLink RED": "CityLink Red",
    "CityLink SILVER": "CityLink Silver",
    "GD": "CityLink Gold",
    "GR": "CityLink Green",
    "LM": "CityLink Lime",
    "NV": "CityLink Navy",
    "OR": "CityLink Orange",
    "PK": "CityLink Pink",
    "PR": "CityLink Purple",
    "RD": "CityLink Red",
    "SV": "CityLink Silver",
    "YW": "CityLink Yellow",
}


# Function to map colors to their full names, keeping unmatched values
def map_color_to_citylink(color):
    return color_to_citylink.get(color, color)


def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


# Function to download MTA bus stops data
@task
def download_mta_bus_stops():
    metadata_url = "https://geodata.md.gov/imap/rest/services/Transportation/MD_Transit/FeatureServer/9?f=pjson"
    metadata_response = requests.get(metadata_url)
    if metadata_response.status_code == 200:
        description = metadata_response.json().get(
            "description", "No description available"
        )
        print("Description from Metadata:", description)
    else:
        print("Failed to retrieve metadata")

    stops = gpd.read_file(
        "https://geodata.md.gov/imap/rest/services/Transportation/MD_Transit/FeatureServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=geojson"
    )
    stops = clean_column_names(stops)
    stops["data_source_description"] = description
    stops["download_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return stops


@task
def transform_mta_bus_stops(gdf):
    gdf["latitude"] = gdf["geometry"].y
    gdf["longitude"] = gdf["geometry"].x
    route_stop = gdf[["stop_id", "routes_served"]].copy()

    # We need to split on commas and semicolons
    route_stop["routes_served"] = route_stop["routes_served"].str.split(",")
    route_stop = route_stop.explode("routes_served")
    # Split on semicolons
    route_stop["routes_served"] = route_stop["routes_served"].str.split(";")
    route_stop = route_stop.explode("routes_served")
    route_stop["routes_served"] = route_stop["routes_served"].str.strip()
    # Apply the function to the 'routes_served' column
    route_stop["routes_served"] = route_stop["routes_served"].apply(
        map_color_to_citylink
    )
    # Re-join the routes served by stop into a df with one row per stop: route_stop, routes_served
    route_stop = (
        route_stop.groupby("stop_id")["routes_served"]
        .apply(list)
        .reset_index()
    )
    # Convert the list of routes served by stop into a string
    route_stop["routes_served"] = route_stop["routes_served"].apply(
        lambda x: ", ".join(x)
    )
    # Drop routes_served from the original gdf
    gdf = gdf.drop(columns=["routes_served"])
    # Merge the routes served by stop back into the original gdf
    gdf = gdf.merge(route_stop, on="stop_id", how="left")
    # Shift routes_served to position 6
    gdf.insert(6, "routes_served", gdf.pop("routes_served"))
    return gdf


@task
def load_mta_bus_stops_to_s3(gdf, s3_bucket, s3_key):
    gdf.to_parquet(f"s3://{s3_bucket}/{s3_key}", index=False)


@flow
def mta_bus_stops_pipeline(s3_bucket, s3_key):
    stops = download_mta_bus_stops()
    stops = transform_mta_bus_stops(stops)
    load_mta_bus_stops_to_s3(stops, s3_bucket, s3_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", type=str, required=True)
    parser.add_argument("--s3-key", type=str, required=True)
    args = parser.parse_args()
    mta_bus_stops_pipeline(s3_bucket=args.s3_bucket, s3_key=args.s3_key)
