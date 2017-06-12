import os, os.path
from xml.dom.minidom import *
import hashlib
from sets import Set
from collections import namedtuple
import copy
from ConflictResolver import ConflictResolver

class HomuraMeta:

    def __init__(self):
        self.mydoc=None
        self.Snapshotdoc=None
        self.HDFSdoc=None
        self.tempdoc=None
        self.myresolver=ConflictResolver()

    def path2Xml(self,path):
        if self.tempdoc!=None:
            print 'Warning! tempdoc is being overwritten'
        try:
            self.tempdoc=Document()
            self.tempdoc.appendChild(self.__path2XmlHelper(path,len(path)))
        except Exception:
            print 'Path Reading Warning: File or directory not exists'
        return

    def __md5(self,fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def __path2XmlHelper(self,path,rootDirLen):
        node=self.tempdoc.createElement('dir')
        if len(path)!=rootDirLen:
            node.setAttribute('path',path[rootDirLen:]+'/')
        else:
            node.setAttribute('path','/')
        for f in os.listdir(path):
            if f[0]=='.':
                continue
            fullpath=os.path.join(path,f)
            timestamp=os.path.getmtime(fullpath)
            if os.path.isdir(fullpath):
                elem=self.__path2XmlHelper(fullpath,rootDirLen)
            else:
                elem=self.tempdoc.createElement('file')
                elem.setAttribute('name',f)
                md5=self.__md5(fullpath)
                elem.setAttribute('md5',md5)
            elem.setAttribute('timestamp',str(timestamp))
            node.appendChild(elem)
        return node

    def saveXml(self,path,Xml='temp'):
        if Xml=='temp':
            my_writer=open(path,'w')
            self.tempdoc.writexml(my_writer)
            my_writer.write('')
            my_writer.close()
        elif Xml=='my':
            my_writer=open(path,'w')
            self.mydoc.writexml(my_writer)
            my_writer.write('')
            my_writer.close()
        elif Xml=='snapshot':
            my_writer=open(path,'w')
            self.Snapshotdoc.writexml(my_writer)
            my_writer.write('')
            my_writer.close()
        elif Xml=='cloud':
            my_writer=open(path,'w')
            self.HDFSdoc.writexml(my_writer)
            my_writer.write('')
            my_writer.close()
        return

    def showHDFSXml(self):
        print self.HDFSdoc.toprettyxml()
    def showMyXml(self):
        print self.mydoc.toprettyxml()
    def showSnapshotXml(self):
        print self.Snapshotdoc.toprettyxml()
    def showTempXml(self):
        print self.tempdoc.toprettyxml()

    def loadMyXml(self,path):
        try:
            myfile=open(path,'r')
            self.mydoc=parseString(myfile.read())
            pass
        except Exception:
            print 'Loading Warning: File or directory not exists'
        return

    def loadSnapshotXml(self,path):
        try:
            myfile=open(path,'r')
            self.Snapshotdoc=parseString(myfile.read())
            pass
        except Exception:
            print 'Loading Warning: File or directory not exists'
        return

    def loadHDFSXml(self,path):
        try:
            myfile=open(path,'r')
            self.HDFSdoc=parseString(myfile.read())
            pass
        except Exception:
            print 'Loading Warning: File or directory not exists'
        return

    def getFileInfoXML(self,XML,filepath):
        return

    #pass the DomNode into the function and get the intersection, used in casualConsistentCompare to get the next part of queue
    #weak standard means the node might be different but we believe two node are same if the names of node are same
    def __intersectByWeakStd(self,children_list_now,children_list_history):
        set_now_dir=Set()
        set1_file=Set()
        set2_file=Set()
        set_history_dir=Set()
        FileStruct = namedtuple("FileStruct", "name md5")
        result1=[]
        result2=[]
        for x in children_list_now:
            if x.nodeName=='dir':
                set_now_dir.add(x.getAttribute('path'))
            if x.nodeName=='file':
                set1_file.add(FileStruct(x.getAttribute('name'), x.getAttribute('md5')))
        for x in children_list_history:
            if x.nodeName=='dir':
                set_history_dir.add(x.getAttribute('path'))
            if x.nodeName=='file':
                set2_file.add(FileStruct(x.getAttribute('name'), x.getAttribute('md5')))
        for x in children_list_now:
            if x.nodeName=='dir' and x.getAttribute('path') in set_history_dir:
                result1.append(x)
            if x.nodeName=='file' and FileStruct(x.getAttribute('name'), x.getAttribute('md5')) in set2_file:
                result1.append(x)
        for x in children_list_history:
            if x.nodeName=='dir' and x.getAttribute('path') in set_now_dir:
                result2.append(x)
            if x.nodeName=='file' and FileStruct(x.getAttribute('name'), x.getAttribute('md5')) in set1_file:
                result2.append(x)
        return (result1,result2)

    def __findOperationInHistory_2(self,children_list_now,children_list_history,createSet,deleteSet,modifySet):#the arguments are two lists of children_node in XML
        set_now_dir=Set()
        set_history_dir=Set()
        dict_now_file={}
        dict_history_file={}
        #put all the stuff in the set and dict
        dirpath2Node_map_now={}
        dirpath2Node_map_history={}
        for x in children_list_now:
            if x.nodeName=='dir':
                set_now_dir.add(x.getAttribute('path'))
                dirpath2Node_map_now[x.getAttribute('path')]=x
            if x.nodeName=='file':
                dict_now_file[x.getAttribute('name')]=x.getAttribute('md5')
        for x in children_list_history:
            if x.nodeName=='dir':
                set_history_dir.add(x.getAttribute('path'))
                dirpath2Node_map_history[x.getAttribute('path')]=x
            if x.nodeName=='file':
                dict_history_file[x.getAttribute('name')]=x.getAttribute('md5')
        #find the operation
        for x in children_list_now:
            if x.nodeName=='dir':
                path=x.getAttribute('path')
                if path not in set_history_dir:
                    node_x=dirpath2Node_map_now[path]
                    self.__BFS_from_node(node_x,createSet)
            if x.nodeName=='file':
                name=x.getAttribute('name')
                md5=x.getAttribute('md5')
                if name not in dict_history_file:
                    createSet.append(x.parentNode.getAttribute('path')+name)
                if name in dict_history_file and dict_history_file[name]!=md5:
                    modifySet.append(x.parentNode.getAttribute('path')+name)
        for x in children_list_history:
            if x.nodeName=='dir':
                path=x.getAttribute('path')
                if path not in set_now_dir:
                    node_x=dirpath2Node_map_history[path]
                    self.__BFS_from_node(node_x,deleteSet)
            if x.nodeName=='file':
                name=x.getAttribute('name')
                if name not in dict_now_file:
                    deleteSet.append(x.parentNode.getAttribute('path')+name)
        return

    def compareMy_Snapshot(self):
        return self.__casualConsistentCompare(self.mydoc,self.Snapshotdoc)

    def compareHDFS_Snapshot(self):
        return self.__casualConsistentCompare(self.HDFSdoc,self.Snapshotdoc)

    #return (create at local, delete at local, modify at local, create at cloud, delete at cloud, modify at cloud)
    def getOperations(self):
        myhistory=list(self.compareMy_Snapshot())
        HDFShistory=list(self.compareHDFS_Snapshot())
        self.myresolver.resolve(HDFShistory,myhistory,'local','hdfs')
        #show operation for debugging
        print '!!!!Operation on local machine:'
        for x in HDFShistory[0]:
            print 'create: '+x
        for x in HDFShistory[1]:
            print 'delete: '+x
        for x in HDFShistory[2]:
            print 'modify: '+x

        print '!!!!Operation on HDFS:'
        for x in myhistory[0]:
            print 'create: '+x
        for x in myhistory[1]:
            print 'delete: '+x
        for x in myhistory[2]:
            print 'modify: '+x
        return (HDFShistory[0],HDFShistory[1],HDFShistory[2],myhistory[0],myhistory[1],myhistory[2])

    #This is the operation the dom1 did in the history!!
    def __casualConsistentCompare(self,dom1,dom2):
        my_q=[dom1]
        Snapshot_q=[dom2]
        createSet=[]
        deleteSet=[]
        modifySet=[]
        while my_q and Snapshot_q:
            my_cur=my_q.pop(0)
            Snapshot_cur=Snapshot_q.pop(0)
            intersect1,intersect2=self.__intersectByWeakStd(my_cur.childNodes,Snapshot_cur.childNodes)
            self.__findOperationInHistory_2(my_cur.childNodes,Snapshot_cur.childNodes,createSet,deleteSet,modifySet)
            my_q+=intersect1
            Snapshot_q+=intersect2
        deleteSet=list(reversed(deleteSet))
        for x in deleteSet:
            print 'delete '+x
        for x in createSet:
            print 'create '+x
        for x in modifySet:
            print 'modify '+x
        return (createSet,deleteSet,modifySet)

    def __BFS_from_node(self,node,some_list):
        q=[node]
        while q:
            cur=q.pop(0)
            q+=cur.childNodes
            if cur.nodeName=='dir':
                some_list.append(cur.getAttribute('path'))
                continue
            if cur.nodeName=='file':
                some_list.append(cur.parentNode.getAttribute('path')+cur.getAttribute('name'))
        return

    def __weakConsistentCompare_not_in_use(self):
        myfile_dict={}
        Snapshotfile_dict={}
        moveSet=Set()#include the tuple of original path and new path
        deleteSet=Set()#include the path of delete
        createSet=Set()
        if self.mydoc:
            for my_file in self.mydoc.getElementsByTagName('file'):
                my_file_md5=my_file.getAttribute('md5')
                my_file_name=my_file.getAttribute('name')
                my_file_dir=my_file.parentNode.getAttribute('path')
                myfile_dict[my_file_md5]=my_file_dir+'/'+my_file_name
        if self.Snapshotdoc:
            for Snapshot_file in self.Snapshotdoc.getElementsByTagName('file'):
                Snapshot_file_md5=Snapshot_file.getAttribute('md5')
                Snapshot_file_name=Snapshot_file.getAttribute('name')
                Snapshot_file_dir=Snapshot_file.parentNode.getAttribute('path')
                Snapshotfile_dict[Snapshot_file_md5]=Snapshot_file_dir+'/'+Snapshot_file_name
        for my_md5 in myfile_dict:
            if  my_md5 in Snapshotfile_dict and myfile_dict[my_md5]==Snapshotfile_dict[my_md5]:
                continue
            if  my_md5 in Snapshotfile_dict and myfile_dict[my_md5]!=Snapshotfile_dict[my_md5]:
                moveSet.add((myfile_dict[my_md5],Snapshotfile_dict[my_md5]))
                continue
            if  my_md5 not in Snapshotfile_dict:
                deleteSet.add(myfile_dict[my_md5])
                continue
        for Snapshot_md5 in Snapshotfile_dict:
            if Snapshot_md5 not in myfile_dict:
                createSet.add(Snapshotfile_dict[Snapshot_md5])

    def __weakConsistentshowOp_not_in_use(self):
        for x in self.moveSet:
            print 'move: '+x[0]+'=>'+x[1]
        for x in self.deleteSet:
            print 'delete: '+x
        for x in self.createSet:
            print 'create: '+x

if __name__ == "__main__":
    my_meta=HomuraMeta()
    my_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/github/sandbox')
    my_meta.showTempXml()
    '''
    my_meta=HomuraMeta()
    my_meta.path2Xml('/Users/xiaoyan/Desktop/local')
    my_meta.saveXml('my.xml')

    my_meta.path2Xml('/Users/xiaoyan/Desktop/checkpoint')
    my_meta.saveXml('history.xml')

    my_meta.path2Xml('/Users/xiaoyan/Desktop/hdfs')
    my_meta.saveXml('cloud.xml')

    my_meta.loadMyXml('my.xml')
    my_meta.loadSnapshotXml('history.xml')
    my_meta.loadHDFSXml('cloud.xml')
    my_meta.showMyXml()
    my_meta.showSnapshotXml()
    my_meta.showHDFSXml()
    my_meta.getOperations()
    '''
