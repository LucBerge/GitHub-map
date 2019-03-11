# GitHub-map

## Description

The GitHub-map project have for objective to create the GitHub community map. Inspired form the [internet-map](http://www.internet-map.net/), we are using the [GitHub kaggle database](https://www.kaggle.com/github/github-repos) to create a simple database. Based on this database, we will try to implement de the PageLinkRank Algorithme to place the nodes on the map.

## Prerequisite

Create a [Google API key](https://cloud.google.com/bigquery/docs/reference/libraries) to query the kaggle database.

Pip installation for **Windows**:
```
python get-pip.py
```
Install the [geckodriver](https://github.com/mozilla/geckodriver/releases) for windows.

Pip installation for **Linux**:
```
sudo apt-get install python-pip
sudo cp geckodriver /usr/local/bin/geckodriver
```

## Installation

Install the dependencies :
```
sudo pip install google-api-python-client requests bs4 selenium sqlite3 mrjob
```
