from hdfs import Config
from homura_meta import HomuraMeta
#from device import existing_dev, add_dev, remove_dev
from monitor import Monitor_Start, Monitor_Stop
import time
import os
import shutil
import sys
import logging


def log(message, error=0):
    if error == 0:
        print 'Log:', message
    else:
        print 'Error:', message


class HomuraFS():

    def __init__(self):
        self.client = Config().get_client('dev')
        self.prompt = 'homura_fs $ '
        self.name = None
        self.local_xml = None
        self.hdfs_xml = '.last_sync.xml'
        self.hdfs_loc_xml = None
        self.mount_root = None #os.getcwd() + '/test'
        self.hdfs_root = '/cs219'
        self.meta = HomuraMeta()
        self.monitor = None
        if sys.platform.startswith('darwin'):
            logging.basicConfig(filename='mylog.log', level=logging.INFO)
            self.monitor = Monitor_Start()
            '''
            for dev in self.monitor.devs:
                devname = dev['Dname']
                manufacture = dev['Man']
                hname = dev['Hname']
                is_sync = raw_input("Devname: {}, Hname: {}, Manufacture: {}. Do you want to sync?\n".format(devname, hname, manufacture))
                #if is_sync == "yes":
                    #self.name = devname
                    #TODO: change the sync root
                    #self.sync_files()
            '''

    def shell_loop(self):
        while True:
            cmd = raw_input(self.prompt)

            if cmd == 'sync':
                print "Current devices attached:"
                id_mapping = dict()
                count = 1

                if len(self.monitor.devs) == 0:
                    print "No device attached"
                    continue 
                for dev in self.monitor.devs:
                    #print dev
                    devname = dev['Dname']
                    manufacture = dev['Man']
                    hname = dev['Hname']
                    id_mapping[count] = dev
                    print "{}) Dname: {}, Hname: {}, Manufacture: {}.\n".format(count, devname, hname, manufacture)
                    count += 1
                dev_id = int(raw_input("Which device to sync:\n"))

                if dev_id == 0:
                    continue

                if dev_id in id_mapping:
                    #self.name = id_mapping[dev_id]['UID']
                    self.name = ''
                    self.mount_root = id_mapping[dev_id]['Path']
                    self.local_xml = self.mount_root + '/.last_sync.xml'
                    self.hdfs_loc_xml = self.mount_root + '/.cur_hdfs.xml'
                    log('Mount root is ' + self.mount_root)
                    log('Device xml file is ' + self.local_xml)
                    log('HDFS xml file is ' + self.hdfs_xml)
                    log('Copy of HDFS xml stored at ' + self.hdfs_loc_xml)
                    log('Syncing files for device ' + id_mapping[dev_id]['Dname'])
                    self.sync_files()
                else:
                    pass

            elif cmd == 'test':
                log('Setting up test directory with default config')
                self.__test()
            elif cmd == 'download':
                pass
                #dl_name = raw_input('Which HDFS directory to download to device? ')
                #log('Downloading HDFS directory ' + dl_name + ' to local device')
                #try:
                #    self.create_file(self.mount_root, dl_name, 1)
                #except:
                #    log('HDFS directory ' + dl_name + ' does not exist')
                #    continue
                #self.meta.path2Xml(self.mount_root)
                #self.meta.saveXml(self.local_xml, Xml='temp')
            elif cmd == 'quit':
                if self.monitor:
                    Monitor_Stop(self.monitor)
                return

    def download_all(self):
        log('Downloading all files from HDFS to local device')
        #try:
        if True:
            self.create_file(self.mount_root, self.hdfs_root, 1)
            for dir_or_file in os.listdir(self.mount_root + self.hdfs_root):
                if not dir_or_file.startswith('.'):
                    shutil.move(self.mount_root + self.hdfs_root + '/' + dir_or_file, self.mount_root)
                    #print "successfully moved file : {}".format(dir_or_file)
            #print 'deleting ' + self.mount_root + self.hdfs_root
            shutil.rmtree(self.mount_root + self.hdfs_root)
        #except:
        #    log('Could not find path in HDFS')
        #    raise Error
        self.meta.path2Xml(self.mount_root)
        self.meta.saveXml(self.local_xml, Xml='temp')

    def upload_all(self):
        log('Uploading all files from local device to HDFS')

        for dir_or_file in os.listdir(self.mount_root):
            if not dir_or_file.startswith('.'):
                try:
                    log('Uploading to ' + self.hdfs_root + '/' + dir_or_file)
                    self.client.upload(self.hdfs_root + '/' + dir_or_file, self.mount_root + '/' + dir_or_file, n_threads=0)
                except:
                    log('Warning: could not upload')

    def load_HDFS_XML(self):
        log("Attempting to fetch HDFS xml")
        self.update_file(self.hdfs_loc_xml, self.hdfs_xml, 1)
        log("Loading HDFS xml")
        self.meta.loadHDFSXml(self.hdfs_loc_xml)
        os.remove(self.hdfs_loc_xml)

    def sync_files(self):
        # check if we have an old snapshot xml
        if not os.path.isfile(self.local_xml): # snapshot doesn't exist, so download everything
            log("No local snapshot file was found at " + self.local_xml)
            self.meta.Snapshotdoc = self.meta.emptyXml() # use empty
            try:
                # fetch HDFS xml and store locally
                self.load_HDFS_XML()

                #self.upload_all()
                #self.download_all()
            except:
                self.HDFSdoc = self.meta.emptyXml()
                #log("Could not find HDFS xml, so uploading everything")
                #if not os.path.isfile(self.local_xml):
                #    with open(self.local_xml, 'w') as writer:
                #        writer.write('') # create dummy xml if not exist
                #self.upload_all()

            #self.meta.path2Xml(self.mount_root)
            #self.meta.saveXml(self.local_xml, Xml='temp')
            #self.update_file(self.local_xml, self.hdfs_xml, 0)
            #return
        else:
            log("Fetching local snapshot xml from " + self.local_xml)
            self.meta.loadSnapshotXml(self.local_xml)

            try:
                # fetch HDFS xml and store locally
                self.load_HDFS_XML()
            except:
                self.meta.HDFSdoc = self.meta.emptyXml()
                #log("--Could not find HDFS xml, so uploading everything")
                #if not os.path.isfile(self.local_xml):
                #    with open(self.local_xml, 'w') as writer:
                #        writer.write('') # create dummy xml if not exist
                #self.upload_all()
            
            #self.meta.path2Xml(self.mount_root)
            #self.meta.saveXml(self.local_xml, Xml='temp')
            #self.update_file(self.local_xml, self.hdfs_xml, 0)
            #return

        #print 'HDFS XML:'
        #self.meta.showHDFSXml()
        #print '---\nSnapshot Xml'
        #self.meta.showSnapshotXml()

        # find operations since last sync
        (my_creates, my_deletes, my_modifies,
        hdfs_creates, hdfs_deletes, hdfs_modifies) = self.meta.getOperations()

        root = self.mount_root
        name = self.hdfs_root

        # apply operations on current device
        for path in my_creates:
            if path.endswith('/'): # path is a folder we want to create
                os.makedirs(root + path)
            else:
                self.create_file(root + path, name + path, 1)
        for path in my_modifies:
            self.update_file(root + path, name + path, 1)
        for path in my_deletes:
            self.delete_file(root + path, 1)

        # apply operations on HDFS
        for path in hdfs_creates:
            if path.endswith('/'): # path is a folder we want to create
                self.client.makedirs(name + path)
            else:
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
            log('Creating ' + hdfs_path + ' on HDFS')
            self.client.upload(hdfs_path, loc_path, n_threads=0)
        elif kyuubey == 1:
            log('Creating ' + loc_path + ' locally')
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
    #name = raw_input('Device name? ')
    fs = HomuraFS()
    fs.shell_loop()
