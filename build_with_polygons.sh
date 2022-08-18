#!/bin/bash

python3 retriever.py ndays 365
python3 lau_fetcher.py
python3 build.py
python3 filler.py
python3 polygons_fetcher.py
