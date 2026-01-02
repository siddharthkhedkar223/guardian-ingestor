"""
main.py — Entry point for The Guardian Ingestor.

Run locally:
    python src/main.py

Run via Docker:
    docker-compose up
"""

import sys
import os

# Ensure src/ is on the module path when run from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()
