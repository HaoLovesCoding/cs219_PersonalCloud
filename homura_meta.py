import os, os.path
from xml.dom.minidom import *
import hashlib
from sets import Set
class HomuraMeta:
	def __init__(self):
		self.mydoc=None
		self.clouddoc=None
		self.moveSet=Set()#include the tuple of original path and new path
		self.deleteSet=Set()#include the path of delete
		self.createSet=Set()

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

	def weakConsistentCompare(self):
		myfile_dict={}
		cloudfile_dict={}
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
				self.moveSet.add((myfile_dict[my_md5],cloudfile_dict[my_md5]))
				continue
			if  my_md5 not in cloudfile_dict:
				self.deleteSet.add(myfile_dict[my_md5])
				continue
		for cloud_md5 in cloudfile_dict:
			if cloud_md5 not in myfile_dict:
				self.createSet.add(cloudfile_dict[cloud_md5])
		#print myfile_dict
		#print cloudfile_dict

	def showOp(self):
		for x in self.moveSet:
			print 'move: '+x[0]+'=>'+x[1]
		for x in self.deleteSet:
			print 'delete: '+x
		for x in self.createSet:
			print 'create: '+x

if __name__ == "__main__":
	my_meta=HomuraMeta()
	my_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/Syncronizer/my_file')
	my_meta.showMyXml()
	
	cloud_meta=HomuraMeta()
	cloud_meta.path2Xml('/Users/HaoWu/Documents/Code/CS219/Syncronizer/cloud_file')
	cloud_meta.saveXml('test.xml')

	my_meta.loadXml('test.xml')
	my_meta.showCloudXml()
	my_meta.weakConsistentCompare()
	my_meta.showOp()