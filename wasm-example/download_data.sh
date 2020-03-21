#!/bin/sh
set -e

rm -rf data
mkdir data

echo Downloading dataset...
wget -O data/titles.tsv.gz https://datasets.imdbws.com/title.basics.tsv.gz
wget -O data/ratings.tsv.gz https://datasets.imdbws.com/title.ratings.tsv.gz

echo Unzipping dataset...
gunzip data/titles.tsv.gz
gunzip data/ratings.tsv.gz

echo All done!