#!/usr/bin/python


# GRAPH with the number of commits

#      6
# A -------- 1
#  \
#   \
#    \
#     \
#      \
#       \ 1
#        \
#         \
#          \
#           \
#            \
#             \
#       2      \
# B ----------- 2
#              /
#             /
#            /
#           /
#          /
#         /
#        /
#       / 3
#      /
#     /
#    /
#   /
#  /      1
# C ----------- 3


# GRAPH with pourcentages

#     <1<
# A -------- 1
#  \  >6/7>
#   \
#    \
#     \
#      \
#       \ <1/6<
#        \
#   >1/7> \
#          \
#           \
#            \
#             \
#      <2/6<   \
# B ----------- 2
#       >1>    /
#             /
#            /
#           /
#          /
#   <3/6< /
#        /
#       / >3/4>
#      /
#     /
#    /
#   /
#  /    <1<
# C ----------- 3
#      >1/4


import sys
sys.path.append("../")
from utils import githubdatabase

db = githubdatabase.GitHubDatabase("test.db")

db.insert_repo("A", None, None, None, None, None, None, None, None, None, None)
db.insert_repo("B", None, None, None, None, None, None, None, None, None, None)
db.insert_repo("C", None, None, None, None, None, None, None, None, None, None)

db.insert_user("1",None,None)
db.insert_user("2",None,None)
db.insert_user("3",None,None)

db.insert_link("A", "1", 6)
db.insert_link("A", "2", 1)
db.insert_link("B", "2", 2)
db.insert_link("C", "2", 3)
db.insert_link("C", "3", 1)

print("\n=== REPOS ===")
print(db.get_repos())

print("\n=== USERS ===")
print(db.get_users())

print("\n=== LINKS ===")
print(db.get_links())

db.close()