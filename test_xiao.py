from ConflictResolver import *


create_a = ["/a/","/a/b/","/a/b/c.txt"]
create_b = ["/a/","/a/d/","/a/b/d.txt"]

delete_a = ["/a/a.pdf","/a/b.pdf","/a/c.pdf"]
delete_b = ["/b/a.pdf","/a/e.pdf","/a/f.pdf"]

modify_a = ["/d/a.pdf","/b/b.pdf","/b/c.pdf"]
modify_b = ["/a/b.pdf","/c/e.pdf","/c/f.pdf"]

list_a = [create_a,delete_a,modify_a]
list_b = [create_b,delete_b,modify_b]

test = ConflictResolver()

result = test.resolve(list_a,list_b)
print "H1: create, delete, modify"
print result[0]
print "H2: create, delete, modify"
print result[1]
