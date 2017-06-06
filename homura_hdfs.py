
from hdfs import Config
from homura_meta import HomuraMeta
import time
import os

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
        self.hdfs_xml = 'name/madoka.xml'
        self.hdfs_loc_xml = 'sayaka.xml'
        self.mount_root = os.getcwd() + '/test'
        self.meta = HomuraMeta()


    def shell_loop(self):
        while True:
            cmd = raw_input(self.prompt)

            if cmd == 'sync':
                log('Syncing files')
                self.sync_files()
            elif cmd == 'quit':
                return


    def sync_files(self):
        # check if we have an old snapshot xml
        if os.path.isfile(self.local_xml):
            self.meta.loadSnapshotXml(self.local_xml)

        # Generate current xml for local
        self.meta.path2Xml(self.mount_root)
        self.meta.mydoc = self.meta.tempdoc

        # fetch HDFS xml and store locally
        self.create_file(self.hdfs_loc_xml, self.hdfs_xml, 1)
        self.meta.loadHDFSXml(self.hdfs_loc_xml)

        # find operations since last sync
        my_ops, hdfs_ops = self.meta.getOperations()

        # apply operations on current device
        creates, deletes, modifies = my_ops
        for loc_path, hdfs_path in creates:
            self.create_file(loc_path, hdfs_path, 1)
        for loc_path, hdfs_path in modifies:
            self.update_file(loc_path, hdfs_path, 1)
        for loc_path, hdfs_path in deletes:
            self.delete_file(loc_path, 1)

        # apply operations on HDFS
        creates, deletes, modifies = hdfs_ops
        for loc_path, hdfs_path in creates:
            self.create_file(loc_path, hdfs_path, 0)
        for loc_path, hdfs_path in modifies:
            self.update_file(loc_path, hdfs_path, 0)
        for loc_path, hdfs_path in deletes:
            self.delete_file(hdfs_path, 0)
                
        # update last sync for both HDFS and current device
        self.meta.path2Xml(self.mount_root)
        self.meta.saveXml(self.local_xml, Xml='temp')
        self.update_file(self, self.local_xml, self.hdfs_xml, 0)

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
            with open(loc_path) as writer:
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


if __name__ == "__main__":
    fs = HomuraFS('pikachu')
    fs.shell_loop()

