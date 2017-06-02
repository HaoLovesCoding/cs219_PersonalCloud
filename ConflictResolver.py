class ConflictResolver(object):

    def resolve(self,list_a,list_b, host_a = "H1", host_b = "H2"):
        #each list has three sublist: create, delete, modify
        delete_a = list_a[1]
        delete_b = list_b[1]
        modify_a = list_a[2]
        modify_b = list_b[2]
        create_a = list_a[0]
        create_b = list_b[0]

        # solve modification conflict

        intersect = filter(lambda x: x in modify_a, modify_b)

        for entry in intersect:
            result = self.user_input(entry, host_a, host_b, "modify", "modify")
            if result == host_a:#leave host_a
                idx = modify_a.index(entry)
                del modify_a[idx]
            elif result == host_b:# leave host_b copy
                idx = modify_b.index(entry)
                del modify_b[idx]

        # solve creation conflict

        intersect = filter(lambda x: x in create_a, create_b)

        for entry in intersect:
            if entry[-1] != "/":#not a folder
                result = self.user_input(entry, host_a, host_b, "create", "create")
                if result == host_a:#leave host_a
                    idx = create_a.index(entry)
                    del create_a[idx]
                elif result == host_b:# leave host_b copy
                    idx = create_b.index(entry)
                    del create_b[idx]
            else:# a folder
                idx1 = create_a.index(entry)
                del create_a[idx1]
                idx2 = create_b.index(entry)
                del create_b[idx2]

        # solve modification-deletion conflict
        # S1: modification from host_a, deletion from host_b
        # It has to be a file because modifications are all files

        intersect = filter(lambda x: x in modify_a, delete_b)

        for entry in intersect:
            result = self.user_input(entry, host_a, host_b, "delete", "modify")
            if result == host_a: # delete
                idx = modify_a.index(entry)
                del modify_a[idx]
            elif result == host_b:# modify
                idx = delete_b.index(entry)
                del delete_b[idx]

        # S2: deletetion from host_a, modification from host_b
        # It has to be a file because modifications are all files

        intersect = filter(lambda x: x in delete_a, modify_b)

        for entry in intersect:
            result = self.user_input(entry, host_a, host_b, "modify", "delete")
            if result == host_a: # modify
                idx = delete_a.index(entry)
                del delete_a[idx]
            elif result == host_b:# delete
                idx = modify_b.index(entry)
                del modify_b[idx]


        rlist_a = [create_a,delete_a,modify_a]
        rlist_b = [create_b,delete_b,modify_b]
        return rlist_a,rlist_b


    def user_input(self,entry,host_a,host_b,type_a,type_b):

        print "There is conflict on a file: " + entry
        print host_a + " wants to " + type_a
        print host_b + " wants to " + type_b
        print "Which host should I listen to?"
        print "usage: input "+ host_a + " or " + host_b

        while True:
             result = raw_input()
             if result == host_a or result == host_b:
                 return result
             else:
                 print "Wrong input"
