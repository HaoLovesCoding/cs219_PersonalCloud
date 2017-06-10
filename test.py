import time
import logging
from monitor import Monitor_Start, Monitor_Stop


if __name__ == "__main__":
	# Exxpect logging in the upper module
	logging.basicConfig(filename='mylog.log', level=logging.INFO)
	monitor = Monitor_Start() 
	time.sleep(15)
	Monitor_Stop(monitor)
