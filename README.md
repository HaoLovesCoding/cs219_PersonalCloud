# cs219_PersonalCloud

## Spring 2017 UCLA with Prof. Songwu Lu  & Taqi 

## Team Member:
Walliam Liu     Willaimlai@ucla.edu
Mushi Zhou		zmushi@gmail.com       
Hao Wu          nanocmp@gmail.com 
Xiao Yan        yanxiao0201@ucla.edu


##  Step to Run Our Program

 * Our middle-ware currently only runs on MacOSX, so a Mac with OSX is needed.    
 * Our middle-ware requires python 2.x. Make sure python 2 version 2.4 or above is available.   
 * Our program requires Node js to run in background for USB detection. Make sure Node is installed using npm.   
 * There are two module dependencies: js2py and USB-Detection. Install them as follows:   
    pip install js2py    
    npm install USB-Detection   
 * In our source repository, execute    
    python homura_hdfs.py

* .....    


REQUIREMENTS:
- Hadoop must be running on the same machine as the device
- the desired namenode server must be specified in your Hadoop configuration file (the 'dev' configuration is used, but can be changed in __init__)
- the filesystem must have some request_file (you can set this in the __init__ function; this request file serves as a log for the requests for files that other clients will make after seeing the metadata
- the 'hdfs' python module must be installed (I used pip and virtualenv)


HOW TO RUN:
`python homura_hdfs.py`
- this will load a shell-like interface
- the following commands are possible:
    `quit` - terminates the program
    `read [hdfs_pathname] - attempts to read file from hdfs; one of three cases are possible:
        file does not exist: error message printed
        metadata exists: client makes request for the actual file to be uploaded and loops until it exists
        actual file exists: client prints contents of file
    `upload [local_pathname] [hdfs_pathname] - uploads metadata for local file to the hdfs path given
- after processing a command, the client will check the request log for any requests made for a file it owns. if so, it will upload the actual file and remove the request entry from the log


TEST FLOW:
Create a file test.txt and type in a few arbitrary lines of text
ClientA runs program using name 'ClientA' (you can set this at the bottom of the python file)
ClientB runs program using name 'ClientB'
ClientA enters command 'upload test.txt /some/path/test.txt'
- a log message should appear indicating the metadata was uploaded
ClientB enters command 'read /some/path/test.txt'
- a log message should appear indicating that metadata exists
- a log message should appear indicating that a new request has been added
ClientA presses enter (acts as a manual "refresh")
- a log message should appear indicating that it is checking the log
- a log message should appear indicating that it is uploading the actual file
ClientB should print out the contents of test.txt after ClientA has uploaded it


NOTES:
- on my machine, HDFS runs really slowly, so having multiple calls to HDFS like in this program is even lower. I'm not sure if this will be an issue, but for now functionality is probably the primary goal
- the current "request log synchronization" is probably not ideal but it was the easiest way for me to connect everything together at the start. We may want to explore alternatives if we have time.
- Please report any bugs that you find!
