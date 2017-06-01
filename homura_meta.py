import os, os.path
from xml.dom.minidom import *
import hashlib
from sets import Set
from collections import namedtuple
import copy

class HomuraMeta:
	DirStruct = namedtuple("DirStruct","path timestamp")

	def __init__(self):
		self.mydoc=None
		self.Snapshotdoc=None

	def path2Xml(self,path):
		try:
			self.mydoc=Document()
			self.mydoc.appendChild(self.__path2XmlHelper(path,len(path)))
		except Exception:
			print 'Path Reading Warning: File or directory not exists'

	def __md5(self,fname):
	    hash_md5 = hashlib.md5()
	    with open(fname, "rb") as f:
	        for chunk in iter(lambda: f.read(4096), b""):
	            hash_md5.update(chunk)
	    return hash_md5.hexdigest()

	def __path2XmlHelper(self,path,rootDirLen):
		node=self.mydoc.createElement('dir')
		if len(path)!=rootDirLen:
			node.setAttribute('path',path[rootDirLen:])
		else:
			node.setAttribute('path','/')
		for f in os.listdir(path):
			fullpath=os.path.join(path,f)
			timestamp=os.path.getmtime(fullpath)
			if os.path.isdir(fullpath):
				elem=self.__path2XmlHelper(fullpath,rootDirLen)
			else:
				elem=self.mydoc.createElement('file')
				elem.setAttribute('name',f)
				md5=self.__md5(fullpath)
				elem.setAttribute('md5',md5)
			elem.setAttribute('timestamp',str(timestamp))
			node.appendChild(elem)
		return node

	def saveXml(self,path):
		my_writer=open(path,'w')
		self.mydoc.writexml(my_writer)
		my_writer.write('')
		my_writer.close()

	def showMyXml(self):
		print self.mydoc.toprettyxml()

	def showSnapshotXml(self):
		print self.Snapshotdoc.toprettyxml()

	def loadXml(self,path):
		try:
			myfile=open(path,'r')
			self.Snapshotdoc=parseString(myfile.read())
			pass
		except Exception:
			print 'Loading Warning: File or directory not exists'

	#pass the DomNode into the function and get the intersection, used in casualConsistentCompare to get the next part of queue
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

	def __findOperationInHistory(self,children_list_now,children_list_history):
		set_now_dir=Set()
		set_history_dir=Set()
		dict_now_file={}
		dict_history_file={}
		createSet=Set()
		deleteSet=Set()
		modifySet=Set()
		#put all the stuff in the set and dict
		for x in children_list_now:
			if x.nodeName=='dir':
				set_now_dir.add(x.getAttribute('path'))
			if x.nodeName=='file':
				dict_now_file[x.getAttribute('name')]=x.getAttribute('md5')
		for x in children_list_history:
			if x.nodeName=='dir':
				set_history_dir.add(x.getAttribute('path'))
			if x.nodeName=='file':
				dict_history_file[x.getAttribute('name')]=x.getAttribute('md5')
		#find the operation
		for x in children_list_now:
			if x.nodeName=='dir':
				path=x.getAttribute('path')
				if path not in set_history_dir:
					createSet.add(x.parentNode.getAttribute('path')+path[1:])
			if x.nodeName=='file':
				name=x.getAttribute('name')
				md5=x.getAttribute('md5')
				if name not in dict_history_file:
					createSet.add(x.parentNode.getAttribute('path')+'/'+name)
				if name in dict_history_file and dict_history_file[name]!=md5:
					modifySet.add(x.parentNode.getAttribute('path')+'/'+name)
		for x in children_list_history:
			if x.nodeName=='dir':
				path=x.getAttribute('path')
				if path not in set_now_dir:
					deleteSet.add(x.parentNode.getAttribute('path')+path[1:])
			if x.nodeName=='file':
				name=x.getAttribute('name')
				if name not in dict_now_file:
					deleteSet.add(x.parentNode.getAttribute('path')+'/'+name)
		#show operation
		for x in deleteSet:
			print 'delete '+x
		for x in createSet:
			print 'create '+x
		for y in modifySet:
			print 'modify '+x


	def casualConsistentCompare(self):
		my_q=[self.mydoc]
		Snapshot_q=[self.Snapshotdoc]
		while my_q and Snapshot_q:
			my_cur=my_q.pop(0)
			Snapshot_cur=Snapshot_q.pop(0)
			if my_cur.nodeName=='file':
				print my_cur.getAttribute('name')
			if Snapshot_cur.nodeName=='dir':
				print Snapshot_cur.getAttribute('path')
			intersect1,intersect2=self.__intersectByWeakStd(my_cur.childNodes,Snapshot_cur.childNodes)
			self.__findOperationInHistory(my_cur.childNodes,Snapshot_cur.childNodes)
			my_q+=intersect1
			Snapshot_q+=intersect2

	def BFS(self):
		q=[self.mydoc]
		while q:
			cur=q.pop(0)
			obj=DomChildrenList(cur.childNodes)
			q+=cur.childNodes
			print cur.childNodes
			obj.show()

	def weakConsistentCompare(self):
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
		#print myfile_dict
		#print Snapshotfile_dict

	def weakConsistentshowOp(self):
		for x in self.moveSet:
			print 'move: '+x[0]+'=>'+x[1]
		for x in self.deleteSet:
			print 'delete: '+x
		for x in self.createSet:
			print 'create: '+x

if __name__ == "__main__":
	my_meta=HomuraMeta()
	my_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/Syncronizer/A')
	my_meta.showMyXml()
	
	Snapshot_meta=HomuraMeta()
	Snapshot_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/Syncronizer/B')
	Snapshot_meta.saveXml('test.xml')

	my_meta.loadXml('test.xml')
	my_meta.showSnapshotXml()
	#my_meta.weakConsistentCompare()
	#my_meta.BFS()
	my_meta.casualConsistentCompare()