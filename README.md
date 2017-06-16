# cs219_PersonalCloud  
## Spring 2017 UCLA with Prof. Songwu Lu & Taqi   

## Team Member:
William Lai     williamlai@ucla.edu   
Hao Wu          nanocmp@gmail.com    
Xiao Yan        yanxiao0201@ucla.edu   
Mushi Zhou		zmushi@ucla.edu         


##  Step-by-Step Guide to Running Our Program

0. We assume use of a MacOSX operating system.
1. Install homebrew 1.2.3
2. Install hadoop 2.7.2
3. Install node 7.10.0
4. Install python 2.7.x
5. Install pip 9.0.1
6. Install npm 4.2.0
7. Install the python HDFS module by running 'pip install hdfs'
8. Install the python js2py module by running 'pip install js2py'
9. Install usb-detection on node by running 'npm install usb-detection'
10. Download our code with the command 'git clone https://github.com/yanxiao0201/cs219_PersonalCloud.git'
11. A configuration file like specified in the python HDFS module documentation is needed (https://hdfscli.readthedocs.io/en/latest/quickstart.html). We provide the configuration file in our repository, but it needs to be copied to your home directory like so: 'cp hdfscli.cfg ~/.hdfscli.cfg'
12. Restart the terminal to ensure that the installations are active.
13. Please confirm that HDFS operations can be executed locally by testing our the creation/upload/deletion/etc. of both files and folders on HDFS.
14. Start HDFS via the command 'hstart' (or equivalent command depending on your configurations)
15. Start up our program via python: 'python homura_hdfs.py'
16. Plug in a USB device if you haven't done so already, and type 'sync' into the prompt. It may take a few seconds for the device to be detected. If you cannot get the device to be shown in the prompt, please make sure that the device is recognized by your computer, as we have had issues before where the system itself did not recognize that a USB had been plugged in. You can check the list of detected devices with the command 'system_profiler SPUSBDataType'
17. Enter the device number that you wish to be synchronized.
18. Check HDFS to see that the files have been uploaded successfully. By default, it should be in a folder named '/cs219'.
19. After you are finished running commands, type 'quit' into the prompt to exit.
