# GitHub-map

## Description

The GitHub-map project have for objective to create the GitHub community map. Inspired form the [internet-map](http://www.internet-map.net/), we are using the [GitHub kaggle database](https://www.kaggle.com/github/github-repos) to create a simple database. Based on this database, we will try to implement de the PageLinkRank Algorithme to place the nodes on the map.

## Overview

- Here is an example of what the algorithm can do. A and B are two repositories whith 50 contributors each. Each user contribute with 1 commit :

![Animated gif](https://media.giphy.com/media/2t9pvY70cVtzctUOGe/giphy.gif)

- 26 repositories with 500 users and 100 links. The number of commits is randomly choose:

![Animated gif](https://media.giphy.com/media/fxq5ertHwZx8stREGc/giphy.gif)

## Database

**repos**

| Column | type | comment |
|-|:-:|:-|
| _repo_name_ | text | Primary key |
| commits | text | Number of commits |
| branches | text | Number of branches |
| releases | text | Number of releases |
| contributors | text | Number of contributors |
| issues | text | Number of opened issues |
| pull_requests | text | Number of openend pull requests |
| watchs | text | Number of viewers |
| starts | text | Number of stars |
| forks | text | Number of forks |
| age | text | Age of the repo |

**users**

| Column | type | comment |
|-|:-:|:-|
| _user_name_ | text | Primary key |
| name | text | Name |
| age | text | Age |

**repos_to_users**

| Column | type | comment |
|-|:-:|:-|
| _user_name_ | text | Foreign key of the repos table |
| _email_ | text | Foreign key of the users table |
| commits | int | Number of commit from the user on the repo |
| issues | int | Number of issues from the user on the repo |

## Prerequisite

Create a [Google API key](https://cloud.google.com/bigquery/docs/reference/libraries) to query the kaggle database.

**Windows**:
- Install pip  ```python get-pip.py```
- Install [geckodriver](https://github.com/mozilla/geckodriver/releases).

**Linux**:
- Install pip ```sudo apt-get install python-pip```
- Copy geckodriver to a sourced path ```sudo cp geckodriver /usr/local/bin/geckodriver```

## Installation

Install the dependencies :
```
sudo pip3 install google-api-python-client requests bs4 selenium sqlite3 mrjob matplotlib
```
## Contributing

- test folder : Only here as a sandbox. It is sometime usefull to try queries on the kaggle database before implementing.
- sample folder : Implementation of the final code with only 100 commits instead of 128 millions
- src folder : Implementation of the final code with every commits

## Results

### Top 10 (20 iterations)
_Links with more than 100 commits_

| Rank | Repo | User |
|:-|:-|:-|
| #1 | MrAlex94/Waterfox (367.31) | Barry Clark (barryclark)(1473.90) |
| #2 | shenzhouzd/update (282.67) | Hakim El Hattab (hakimel)() |
| #3 | jrobhoward/SCADAbase (112.32) | clowwindy (clowwindy)(758.55) |
| #4 | kunj1988/Magento2 (8640) | Michael Rose (715.00) |
| #5 | xorware/android_frameworks_base (80.73) | Gerrit Code Review (580.55) |
| #6 | tarunprabhu/DragonEgg (76.53) | Will Farrington (528.61) |
| #7 | meego-tablet-ux/meego-app-browser (44.63) | Guillermo Rauch (rauchg)(409.58) |
| #8 | wistoch/meego-app-browser (43.68) | Fabien Potencier (408.16) |
| #9 | xen2/mcs (41.96) | Durran Jordan (359.46) |
| #10 | orumin/openbsd-efivars (40.46) | Robby Russell (robbyrussell)(355.38) |
