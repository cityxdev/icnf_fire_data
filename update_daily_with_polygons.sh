#!/bin/bash

python3 retriever.py ndays 30
python3 filler.py ndays 30
python3 polygons_fetcher.py ndays 30
