import os

class ConflictResolver(object):

    def __init__(self):
        self.local_root = None

    def resolve(self,list_a,list_b, host_a = "Local", host_b = "HDFS"):
        #each list has three sublist: create, delete, modify
        delete_a = list_a[1]
        delete_b = list_b[1]
        modify_a = list_a[2]
        modify_b = list_b[2]
        create_a = list_a[0]
        create_b = list_b[0]

        # solve deletion conflict

        intersect = filter(lambda x: x in delete_a, delete_b)

        for entry in intersect:
            idx1 = delete_a.index(entry)
            del delete_a[idx1]
            idx2 = delete_b.index(entry)
            del delete_b[idx2]


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

            elif (result == "Both") or (result == "both"):
                (local_dirpath,old_filename) = self.find_local_dir(entry)
                if old_filename not in os.listdir(local_dirpath):
                    raise NameError, old_filename + " is not on the local computer"
                else:
                    old_fullname = local_dirpath + os.path.sep + old_filename
                    new_fullname, new_filename = self.recFindname(old_filename, host_a, local_dirpath)
                    os.rename(old_fullname, new_fullname)

                    idx = modify_b.index(entry)
                    del modify_b[idx]

                    #create a new file named new name
                    new_entry = self.generate_entry(entry,new_filename)
                    create_b.append(new_entry)

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

                elif (result == "Both") or (result == "both"):
                    (local_dirpath,old_filename) = self.find_local_dir(entry)
                    if old_filename not in os.listdir(local_dirpath):
                        raise NameError, old_filename + " is not on the local computer"
                    else:
                        old_fullname = local_dirpath + os.path.sep + old_filename
                        new_fullname, new_filename = self.recFindname(old_filename, host_a, local_dirpath)
                        os.rename(old_fullname, new_fullname)

                        idx = create_b.index(entry)
                        del create_b[idx]

                        #create a new file named new name
                        new_entry = self.generate_entry(entry,new_filename)
                        create_b.append(new_entry)


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
        print ""
        print "There is conflict on a file: " + entry
        print host_a + " wants to " + type_a
        print host_b + " wants to " + type_b
        print "Which host should I listen to?"
        print "usage: input "+ host_a + " or " + host_b

        while True:
             result = raw_input("Answer:")
             if result == host_a or result == host_b:
                 return result
            #TODO: Add support for keeping both copy of the files
             elif ((result == "Both") or (result == "both")) and type_a == type_b:
                 return result
             else:
                 print "Wrong input"

    def set_local_root(self,root):
        if root[0] != "/":
            raise ValueError, "The root should start with \"/\""
        elif root[-1] == "/":
            root = root[:-1]
        self.local_root = root

    def find_local_dir(self,entry):
        if self.root == None:
            raise ValueError, "local root directory has not been set"
        else:
            local_filepath = self.root + entry
            for i in range(len(local_filepath)-1,-1,-1):
                if local_filepath[i] == "/":
                    break
            local_dirpath = local_filepath[:i]
            filename = local_filepath[i+1:]
            return (local_dirpath,filename)

    def recFindname(self,old_filename, host_a, local_dirpath):
        new_filename = old_filename
        while True:
            new_filename += "_" + host_a
            if new_filename not in os.listdir(local_dirpath):
                break
        new_fullname = local_dirpath + new_filename
        return new_fullname, new_filename

    def generate_entry(self,entry,new_filename):
        for i in xrange(len(entry)-1, -1, -1):
            if entry[i] == "/":
                break
        dir_path = entry[:i]
        new_entry = dir_path + os.path.sep + new_filename
        return new_entry
