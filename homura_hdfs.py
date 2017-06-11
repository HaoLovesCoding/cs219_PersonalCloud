
from hdfs import Config
from homura_meta import HomuraMeta
#from device import existing_dev, add_dev, remove_dev
#from monitor import Monitor_Start, Monitor_Stop
import time
import os
import shutil
import sys

def log(message, error=0):
    if error == 0:
        print 'Log:', message
    else:
        print 'Error:', message


class HomuraFS():

    def __init__(self, name):
        self.client = Config().get_client('dev')
        self.prompt = 'homura_fs $ '
        self.name = name
        self.local_xml = 'madoka.xml'
        self.hdfs_xml = name + '/madoka.xml'
        self.hdfs_loc_xml = 'sayaka.xml'
        self.mount_root = os.getcwd() + '/test'
        self.meta = HomuraMeta()
        self.monitor = None
        #if sys.platform.startswith('darwin'):
        #    self.monitor = Monitor_Start()

    def shell_loop(self):
        while True:
            cmd = raw_input(self.prompt)

            if cmd == 'sync':
                log('Syncing files')
                self.sync_files()
            if cmd == 'test':
                log('Setting up test directory with default config')
                self.__test()
            elif cmd == 'quit':
                #if self.monitor:
                #    Monitor_Stop(self.monitor)
                return

    def sync_files(self):
        # check if we have an old snapshot xml
        if os.path.isfile(self.local_xml):
            self.meta.loadSnapshotXml(self.local_xml)

        # Generate current xml for local
        self.meta.path2Xml(self.mount_root)
        self.meta.mydoc = self.meta.tempdoc

        # fetch HDFS xml and store locally
        try:
            log("Attempting to fetch HDFS xml")
            self.update_file(self.hdfs_loc_xml, self.hdfs_xml, 1)
            log("Loaded HDFS xml")
            self.meta.loadHDFSXml(self.hdfs_loc_xml)
        except:
            # download entire folder and update
            self.client.upload(self.name, self.mount_root, n_threads=0)
            self.meta.path2Xml(self.mount_root)
            self.meta.saveXml(self.local_xml, Xml='temp')
            self.create_file(self.local_xml, self.hdfs_xml, 0)
            return

        # find operations since last sync
        (my_creates, my_deletes, my_modifies, 
        hdfs_creates, hdfs_deletes, hdfs_modifies) = self.meta.getOperations()

        root = self.mount_root
        name = self.name

        # apply operations on current device
        for path in my_creates:
            self.create_file(root + path, name + path, 1)
        for path in my_modifies:
            self.update_file(root + path, name + path, 1)
        for path in my_deletes:
            self.delete_file(root + path, 1)

        # apply operations on HDFS
        for path in hdfs_creates:
            self.create_file(root + path, name + path, 0)
        for path in hdfs_modifies:
            self.update_file(root + path, name + path, 0)
        for path in hdfs_deletes:
            self.delete_file(name + path, 0)

        # update last sync for both HDFS and current device
        self.meta.path2Xml(self.mount_root)
        self.meta.saveXml(self.local_xml, Xml='temp')
        self.update_file(self.local_xml, self.hdfs_xml, 0)

        return


    # in this set of functions, when kyuubey = 0, the operation goes
    # from loc to hdfs (i.e. local becomes the "master")
    # when kyuubey = 1, the operation goes from hdfs to loc
    # (i.e. hdfs becomes the "master")
    def create_file(self, loc_path, hdfs_path, kyuubey):
        if kyuubey == 0:
            log('Creating file ' + hdfs_path + ' on HDFS')
            self.client.upload(hdfs_path, loc_path, n_threads=0)
        elif kyuubey == 1:
            log('Creating file ' + loc_path + ' locally')
            self.client.download(hdfs_path, loc_path, n_threads=0)

    def update_file(self, loc_path, hdfs_path, kyuubey):
        if kyuubey == 0: # updating file on HDFS
            log('Updating file ' + hdfs_path + ' on HDFS')
            with open(loc_path) as reader:
                with self.client.write(hdfs_path, overwrite=True) as writer:
                    for line in reader:
                        writer.write(line)
        elif kyuubey == 1:
            log('Updating file ' + loc_path + ' locally')
            with open(loc_path, 'w') as writer:
                with self.client.read(hdfs_path) as reader:
                    data = reader.read()
                    writer.write(data)

    def delete_file(self, path, kyuubey):
        if kyuubey == 0: # delete file on HDFS
            log('Deleting file ' + path + ' from HDFS')
            self.client.delete(path, recursive=True)
        elif kyuubey == 1: # delete file locally
            log('Deleting file ' + path + ' locally')
            os.remove(path)
            
    def move_file(self, src_path, dst_path, kyuubey):
        if kyuubey == 0: # move file on HDFS
            log('Moving file from ' + src_path + ' to ' + dst_path + ' on HDFS')
            self.client.rename(src_path, dst_path)
        elif kyuubey == 1: # move file locally
            os.rename(src_path, dst_path)
            log('Moving file from ' + src_path + ' to ' + dst_path + ' locally')

    def __test(self, test_no=1):
        self.__reset_test()
        if test_no == 1:
            self.__config_basic()
        elif test_no == 2:
            self.__config_outer_empty()

    def __reset_test(self):
        root = self.mount_root
        log('Resetting mount directory')
        if os.path.exists(root):
            shutil.rmtree(root)
        os.makedirs(root)
        
    def __config_basic(self):
        root = self.mount_root
        log('Config 1: default')
        with open(root + '/test1.txt', 'w') as writer:
            writer.write('hi\nthere\n!\n')
        with open(root + '/test2.txt', 'w') as writer:
            writer.write('one-liner')
        with open(root + '/test3.txt', 'w') as writer:
            writer.write('')
        os.makedirs(root + '/subdir')
        with open(root + '/subdir/test1.txt', 'w') as writer:
            writer.write('a different\ntest1.txt\nfile!\n')
        os.makedirs(root + '/subdir/subsubdir')
        with open(root + '/subdir/subsubdir/test1.txt', 'w') as writer:
            writer.write('yet another different\ntest1.txt\nfile!\n')

    def __config_outer_empty(self):
        root = self.mount_root
        log('Config 2: outer directory empty')
        os.makedirs(root + '/subdir')
        with open(root + '/subdir/test1.txt', 'w') as writer:
            writer.write('a different\ntest1.txt\nfile!\n')
        os.makedirs(root + '/subdir/subsubdir')
        with open(root + '/subdir/subsubdir/test1.txt', 'w') as writer:
            writer.write('yet another different\ntest1.txt\nfile!\n')



if __name__ == "__main__":
    fs = HomuraFS('pikachu')
    fs.shell_loop()

