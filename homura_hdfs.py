
from hdfs import Config
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

    def shell_loop(self):
        while True:
            cmd = raw_input(self.prompt)

            if cmd == 'sync':
                log('Syncing files')
                self.sync_files()
            elif cmd == 'quit':
                return

    def sync_files(self):
        # fetch XML file of last sync
        # generate current XML file of device
        # find operations between last sync and current device
        # fetch current XML file of HDFS
        # find operations between last sync and current HDFS
        # apply operations on current device
        # apply operations on HDFS
        # update last sync for both HDFS and current device
        pass

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

