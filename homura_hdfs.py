
from hdfs import Config
import time


class HomuraFS():

    def __init__(self, name):
        self.client = Config().get_client('dev')
        self.request_file = 'hadooptest/request_log.txt'
        self.prompt = 'homura_fs $ '
        self.name = name

    def shell_loop(self):
        while True:
            cmd = raw_input(self.prompt).split(' ')

            if len(cmd) == 1 and cmd[0] == 'quit':
                break
            elif len(cmd) == 1 and cmd[0] == 'help':
                print 'placeholder for help'
            elif len(cmd) == 1 and cmd[0] == 'ls':
                print 'placeholder for ls'
            elif cmd[0] == 'read':
                filepath = cmd[1]
                self.handle_read(filepath)
            elif cmd[0] == 'upload':
                filepath = cmd[1]
                hadoop_path = cmd[2]
                self.handle_upload(filepath, hadoop_path, 0) # don't copy actual file

            self.check_request_log()

    def check_request_log(self):
        print 'Log: Checking request log'
        with self.client.read(self.request_file) as reader:
            lines = reader.read().split('\n')
            buf = ''
            for line in lines:
                if line == '':
                    continue
                host, origin_path, hadoop_path = line.split(',')
                if host == self.name:
                    self.handle_upload(origin_path, hadoop_path, 1)
                else:
                    buf += line + '\n'
            self.client.write(self.request_file, overwrite=True, data=buf, append=False)

    def handle_upload(self, filepath, hadoop_path, upload_actual):
        with open(filepath) as reader:
            with self.client.write(hadoop_path, overwrite=True) as writer:
                if upload_actual:
                    print 'Log: Uploading actual file'
                    for line in reader:
                        writer.write(line)
                else:
                    print 'Log: Uploading metadata'
                    writer.write('METADATA\n')
                    writer.write(self.name + '\n')
                    writer.write(filepath + '\n')

    def handle_read(self, filepath):
        while True:
            exists = self.exists_file(filepath)
            if exists == 'DNE':
                print 'Error: Could not find file with name ' + filepath
                return
            elif exists == 'METADATA':
                print 'Log: Only metadata exists right now'
                with self.client.read(filepath) as reader:
                    data = reader.read()
                    lines = data.split('\n')
                    host = lines[1]
                    origin_path = lines[2]
                    req = host + ',' + origin_path + ',' + filepath + '\n'
                    exists_req = False
                    with self.client.read(self.request_file) as reader2:
                        lines = reader2.read().split('\n')
                        for line in lines:
                            if req == line + '\n': # request already in
                                print 'Log: Request exists already'
                                exists_req = True
                                break
                    if not exists_req:
                        self.client.write(self.request_file, data=req, append=True)
                        print 'Log: Request entered into log'

            else:
                print exists # contains data otherwise
                return
            time.sleep(2) # sleep between polls

    def exists_file(self, filepath):
        try:
            with self.client.read(filepath) as reader:
                data = reader.read()
                lines = data.split('\n')
                if lines[0] == 'METADATA':
                    return 'METADATA' # only metadata
                else:
                    return data # data exists
        except:
            return 'DNE' # file does not exist


if __name__ == "__main__":
    fs = HomuraFS('pikachu')
    fs.shell_loop()

