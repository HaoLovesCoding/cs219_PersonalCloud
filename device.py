# Since USB devices mouting procedures are OS dependent,
# This implementation only works on MacOX
# This works with any python > 2.4 (2.x)
import subprocess
import js2py
from plistlib import readPlistFromString

# This function returns the already mounted path of external devices
# def existing_device():
#     mounted = {}

#     output = subprocess.check_output(['ls', '/Volumes'])

#     devs = output.split("\n")

#     for dev in devs:
#         if (dev != "Macintosh HD" and dev != ''):
#             info = subprocess.check_output(['diskutil', 'info', '/Volumes/' + dev])
#             info = info.split('\n')
#             di = {}
#             di['Read-Only'] = 'No'
#             di['Path'] = 'Volumes/' + dev + '/'
#             for line in info:
#                 if (line != ''):
#                     pairs = line.split(":")
#                     if (pairs[0].strip() == 'Volume UUID'):
#                         di['ID'] = pairs[1].strip()
#                     if (pairs[0].strip() == 'Read-Only Media' or pairs[0].strip() == 'Read-Only Volume'):
#                         if (pairs[1]).strip() == 'Yes':
#                             di['Read-Only'] = 'Yes'
#             mounted[dev] = di
    
#     return mounted



def existing_dev():
    mounted = []
    info = subprocess.check_output(['system_profiler', 'SPUSBDataType', '-xml'])
    struct = readPlistFromString(info)
    for i in struct:
        items = i['_items']
        for item in items:
            if '_items' in item:
                ilist = item['_items']
                for media in ilist:
                    if 'Volumes' in media:
                        for volume in media['Volumes']:
                            di = {}
                            di['Hname'] = media['_name']
                            di['Man'] = media['manufacturer']
                            di['PID'] = str(int(media['product_id'], 0))
                            di['VID'] = str(int(media['vendor_id'].split(" ")[0], 0))
                            di['Dname'] = volume['_name']
                            di['Path'] = volume['mount_point']
                            di['UID'] = volume['volume_uuid']
                            mounted.append(di)
                    if 'Media' in media:
                        for partition in media['Media']:
                            for volume in partition['volumes']:
                                di = {}
                                di['Hname'] = media['_name']
                                di['Man'] = media['manufacturer']
                                di['PID'] = str(int(media['product_id'], 0))
                                di['VID'] = str(int(media['vendor_id'].split(" ")[0], 0))
                                di['Dname'] = volume['_name']
                                di['Path'] = volume['mount_point']
                                di['UID'] = volume['volume_uuid']
                                mounted.append(di)
    return mounted

def add_dev(dev, devs):
    ds = []
    info = subprocess.check_output(['system_profiler', 'SPUSBDataType', '-xml'])
    struct = readPlistFromString(info)
    for i in struct:
        items = i['_items']
        for item in items:
            if '_items' in item:
                ilist = item['_items']
                for media in ilist:
                    if 'Volume' in media:
                        if (dev['PID'] == str(int(media['product_id'],0)) and dev['VID'] == str(int(media['vendor_id'].split(" ")[0],0))): 
                            for volume in ilist['volumes']:
                                di = {}
                                di['Hname'] = media['_name']
                                di['Man'] = media['manufacturer']
                                di['PID'] = str(int(media['product_id'], 0))
                                di['VID'] = str(int(media['vendor_id'].split(" ")[0], 0))
                                di['Dname'] = volume['_name']
                                di['Path'] = volume['mount_point']
                                di['UID'] = volume['volume_uuid']
                                devs.append(di)
                                ds.append(di['Dname'])
                    if 'Media' in media:
                        if (dev['PID'] == str(int(media['product_id'],0)) and dev['VID'] == str(int(media['vendor_id'].split(" ")[0],0))): 
                            for partition in media['Media']:
                                for volume in partition['volumes']:
                                    di = {}
                                    di['Hname'] = media['_name']
                                    di['Man'] = media['manufacturer']
                                    di['PID'] = str(int(media['product_id'], 0))
                                    di['VID'] = str(int(media['vendor_id'].split(" ")[0], 0))
                                    di['Dname'] = volume['_name']
                                    di['Path'] = volume['mount_point']
                                    di['UID'] = volume['volume_uuid']
                                    devs.append(di)
                                    ds.append(di['Dname'])
    return ds

def remove_dev(dev, devs):
    ds = []
    for di in devs:
        if (di['VID'] == dev['VID'] and di['PID'] == dev['PID']):
            devs.remove(di)
            ds.append(di['Dname'])
    return ds





