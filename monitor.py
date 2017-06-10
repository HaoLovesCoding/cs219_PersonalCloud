# This works on all UNIX systems
# This can work on Windows with slight modification to the non-blocking I/O set up
# This works with any python > 2.4 (2.x)


import os
import fcntl
import threading
import subprocess
import logging
import time
from device import existing_dev
from device import add_dev
from device import remove_dev

class devClass(threading.Thread):	
	stop = False
	devs = {}

	def __init__(self):
		self.stdout = None
		self.stderr = None
		threading.Thread.__init__(self)
		self.devs = existing_dev()

	def run(self):
		logging.info("Start monitoring")
		devs = existing_dev()
		output = subprocess.Popen(['node', 'auto_detect.js'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

		fd = output.stdout.fileno()
		fl = fcntl.fcntl(fd, fcntl.F_GETFL)
		fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

		while True:
			if (self.stop == True):
				output.communicate("stop")
				logging.info("Stopped monitoring")
				break

			try: nextline = output.stdout.readline()
			except: continue
			# if (nextline == '' and output.poll() is not None):
			# 	break
			
			nextline = nextline.split(',')
			if (nextline[0] == 'add' or nextline[0] == 'remove'):
				dev = {}
				dev['VID'] = nextline[1]
				dev['PID'] = nextline[2]
				dev['man'] = nextline[3]
				dev['name'] = nextline[4].strip()

				if (nextline[0] == 'add'):
					time.sleep(3)
					ds = add_dev(dev, self.devs)
					for volume in ds:
						logging.info('Mounted device ' + dev['name'] + ' volume ' + volume +  ' manufactured by ' + dev['man'])
				
				elif (nextline[0] == 'remove'):
					ds = remove_dev(dev, self.devs)
					for volume in ds:
						logging.info('Unmounted device ' + dev['name'] + ' volume ' + volume +  ' manufactured by ' + dev['man'])

# API
def Monitor_Start():
	dev_class = devClass()
	dev_class.start()
	return dev_class


def Monitor_Stop(dev_class):
	dev_class.stop = True
	dev_class.join()

