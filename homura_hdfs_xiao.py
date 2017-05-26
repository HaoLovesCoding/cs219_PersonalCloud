from hdfs import Config
import time
import socket
import sys


class HomuraFS():

    def __init__(self, name, portNo):
        self.client = Config().get_client('dev')
        self.request_file = 'hadooptest/request_log.txt'
        self.prompt = 'homura_fs $ '
        self.name = name

        # for TCP/IP connection
        self.portNo = portNo
        self.testfile = "test.txt"
        self.addr = "localhost"

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
                self.handle_readTCP(filepath)


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


    def TCPServer(self):
        # build a TCP connection with the SD card
        # waiting for TCP calls
        s = socket.socket()         # Create a socket object
        host = socket.gethostname() # Get local machine name
        port = self.portNo                # Reserve a port for your service.
        s.bind((host, port))        # Bind to the port

        s.listen(5)                 # Now wait for client connection.
        while True:
           c, addr = s.accept()     # Establish connection with client.
           print 'Got connection from', addr

           message = c.recv(1024)
           print "message: {}".format(message)
           message = message.split(":")
           #print message

           filename = message[-1].strip('\n')
           print 'Filename: {}'.format(filename)
           upload = self.handle_uploadTCP(filename,filename,1)

           c.send(upload)
           c.close()                # Close the connection

    def TCPClient(self, filename, server_port):
        # building a TCP connection to the server

        s = socket.socket()         # Create a socket object
        host = socket.gethostname() # Get local machine name
        port = server_port

        s.connect((host, port))
        s.send("Please upload the file:{}".format(filename))
        recv_msg = s.recv(1024)

        s.close()

        return recv_msg

    def server_program(self):

        hadoop_path = self.testfile
        filepath = self.testfile
        self.handle_uploadTCP(filepath,hadoop_path,0)
        self.TCPServer()


    def handle_uploadTCP(self, filepath, hadoop_path, upload_actual):
        try:
            with open(filepath) as reader:
                with self.client.write(hadoop_path, overwrite=True) as writer:
                    if upload_actual:
                        print 'Log: Uploading actual file'
                        for line in reader:
                            writer.write(line)
                        return "Success"
                    else:
                        print 'Log: Uploading metadata'
                        writer.write('METADATA\n')
                        writer.write(self.name + '\n')
                        writer.write(str(self.portNo) + '\n')
                        writer.write(self.addr + '\n')
                        print 'Uploading metadata done'
                        return "Metadata"
        except:
            print "Error: Server cannot upload file"
            return "Error: Server cannot upload file"

    def handle_readTCP(self, filepath):

        while (True):

            exists = self.exists_file(filepath)
            if exists == 'DNE':
                print 'Error: Could not find file with name ' + filepath
                return
            elif exists == 'METADATA':
                print 'Log: Only metadata exists right now'
                with self.client.read(filepath) as reader:
                    data = reader.read()
                    lines = data.split('\n')
                    filename = filepath
                    portNo = int(lines[2])
                    server_addr = lines[3]

                    recv_msg = self.TCPClient(filename, portNo)

                    if recv_msg == "Success":
                        continue
                    else:
                        print recv_msg
                        print "Filename: {}".format(filename)
                        return

            else:
                print exists # contains data otherwise
                return



if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "usage: python [filename.py] [server/client] [portNo]"
    else:
        prog_type = sys.argv[1]
        portNo = int(sys.argv[2])

        fs = HomuraFS('pikachu',portNo)

        if prog_type == "server":
            fs.server_program()

        elif prog_type == "client":
            fs.shell_loop()
