#!/bin/bash

python3 retriever.py ndays 365
python3 filler.py ndays 365
python3 polygons_fetcher.py ndays 365
