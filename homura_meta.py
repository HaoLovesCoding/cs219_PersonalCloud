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
		self.clouddoc=None

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

	def showCloudXml(self):
		print self.clouddoc.toprettyxml()

	def loadXml(self,path):
		try:
			myfile=open(path,'r')
			self.clouddoc=parseString(myfile.read())
			pass
		except Exception:
			print 'Loading Warning: File or directory not exists'

	#pass the DomNode into the function and get the intersection by the standard
	def intersectByWeakStd(self,children_list1,children_list2):
		set1_dir=Set()
		set1_file=Set()
		set2_file=Set()
		set2_dir=Set()
		FileStruct = namedtuple("FileStruct", "name md5")
		result1=[]
		result2=[]
		for x in children_list1:
			if x.nodeName=='dir':
				set1_dir.add(x.getAttribute('path'))
			if x.nodeName=='file':
				set1_file.add(FileStruct(x.getAttribute('name'), x.getAttribute('md5')))
		for x in children_list2:
			if x.nodeName=='dir':
				set2_dir.add(x.getAttribute('path'))
			if x.nodeName=='file':
				set2_file.add(FileStruct(x.getAttribute('name'), x.getAttribute('md5')))
		for x in children_list1:
			if x.nodeName=='dir' and x.getAttribute('path') in set2_dir:
				result1.append(x)
			if x.nodeName=='file' and FileStruct(x.getAttribute('name'), x.getAttribute('md5')) in set2_file:
				result1.append(x)
		for x in children_list2:
			if x.nodeName=='dir' and x.getAttribute('path') in set1_dir:
				result2.append(x)
			if x.nodeName=='file' and FileStruct(x.getAttribute('name'), x.getAttribute('md5')) in set1_file:
				result2.append(x)
		return (result1,result2)

	def casualConsistentCompare(self):
		my_q=[self.mydoc]
		cloud_q=[self.clouddoc]
		while my_q and cloud_q:
			my_cur=my_q.pop(0)
			cloud_cur=cloud_q.pop(0)
			if my_cur.nodeName=='file':
				print my_cur.getAttribute('name')
			if cloud_cur.nodeName=='dir':
				print cloud_cur.getAttribute('path')
			intersect1,intersect2=self.intersectByWeakStd(my_cur.childNodes,cloud_cur.childNodes)
			my_q+=intersect1
			cloud_q+=intersect2

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
		cloudfile_dict={}
		moveSet=Set()#include the tuple of original path and new path
		deleteSet=Set()#include the path of delete
		createSet=Set()
		if self.mydoc:
			for my_file in self.mydoc.getElementsByTagName('file'):
				my_file_md5=my_file.getAttribute('md5')
				my_file_name=my_file.getAttribute('name')
				my_file_dir=my_file.parentNode.getAttribute('path')
				myfile_dict[my_file_md5]=my_file_dir+'/'+my_file_name
		if self.clouddoc:
			for cloud_file in self.clouddoc.getElementsByTagName('file'):
				cloud_file_md5=cloud_file.getAttribute('md5')
				cloud_file_name=cloud_file.getAttribute('name')
				cloud_file_dir=cloud_file.parentNode.getAttribute('path')
				cloudfile_dict[cloud_file_md5]=cloud_file_dir+'/'+cloud_file_name
		for my_md5 in myfile_dict:
			if  my_md5 in cloudfile_dict and myfile_dict[my_md5]==cloudfile_dict[my_md5]:
				continue
			if  my_md5 in cloudfile_dict and myfile_dict[my_md5]!=cloudfile_dict[my_md5]:
				moveSet.add((myfile_dict[my_md5],cloudfile_dict[my_md5]))
				continue
			if  my_md5 not in cloudfile_dict:
				deleteSet.add(myfile_dict[my_md5])
				continue
		for cloud_md5 in cloudfile_dict:
			if cloud_md5 not in myfile_dict:
				createSet.add(cloudfile_dict[cloud_md5])
		#print myfile_dict
		#print cloudfile_dict

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
	
	cloud_meta=HomuraMeta()
	cloud_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/Syncronizer/B')
	cloud_meta.saveXml('test.xml')

	my_meta.loadXml('test.xml')
	my_meta.showCloudXml()
	#my_meta.weakConsistentCompare()
	#my_meta.BFS()
	my_meta.casualConsistentCompare()