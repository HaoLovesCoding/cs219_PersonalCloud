TCP connection design:

1) Right now there are two modes of the program
   a) server mode:
   serve the files of the SD card, on start, it will scan and upload a metafile for the files on SD card, to HDFS. My default root folder is /cs219/. Yours may be different depending on the configuration
   In a terminal session, type:
   python your_source_code.py server portNo

   b) client mode:
   this is the main program for the queries. 
   In a different terminal session, type:
   python your_source_code.py client server_portNo
   
 
