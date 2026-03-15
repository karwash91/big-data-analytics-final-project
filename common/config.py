"""Shared environment and path settings."""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR.joinpath(".env"))

POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = int(os.environ["POSTGRES_PORT"])

KAFKA_BOOTSTRAP_SERVERS = [
    server.strip() for server in os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
]
KAFKA_CONSUMER_GROUP = os.environ["KAFKA_CONSUMER_GROUP"]

DATA_DIR = ROOT_DIR.joinpath("data")
OUTPUTS_DIR = ROOT_DIR.joinpath("outputs")
