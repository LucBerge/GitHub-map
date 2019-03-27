#!/usr/bin/python

from utils import githubdatabase

db = githubdatabase.GitHubDatabase("test.db")

db.insert_repo("myrepo", "524", "151", "2", "41", "24", "21", "215", "141", "426", "2")
db.insert_user("aaa","Ali","34")
db.insert_link("myrepo", "aaa", 5)
db.insert_link("myrepo", "aaa")

print(db.find_repo("myrepo"))
print(db.find_user("aaa"))
print(db.find_link("myrepo", "aaa"))

db.close()