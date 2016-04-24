#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import os.path

from fuse import FUSE, FuseOSError, Operations
from time import time
from azure.storage import CloudStorageAccount
from tests import (
	BlobSasSamples,
	ContainerSamples,
	BlockBlobSamples,
	AppendBlobSamples,
	PageBlobSamples,
)

from azure.storage.blob import BlockBlobService
import azure.storage.blob

import pdb

debug = True 

class Passthrough(Operations):
	def __init__(self, root):
		self.root = root
		print root
		try:
			import config as config
		except:
			raise ValueError('Please specify configuration settings in config.py.')

		if config.IS_EMULATED:
			self.account = CloudStorageAccount(is_emulated=True)
		else:
			# Note that account key and sas should not both be included
			account_name = config.STORAGE_ACCOUNT_NAME
			account_key = config.STORAGE_ACCOUNT_KEY
			sas = config.SAS
			self.account = CloudStorageAccount(account_name=account_name, 
											   account_key=account_key, 
											   sas_token=sas)
			self.service = self.account.create_block_blob_service()


	def _full_path(self, partial):
		if partial.startswith("/"):
			partial = partial[1:]
		path = os.path.join(self.root, partial)
		return path

	def _get_container_reference(self, prefix='container'):
		return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

	def access(self, path, mode):
		if debug:
			print "access" 

		full_path = self._full_path(path)
		#if not os.access(full_path, mode):
		#	pass#raise FuseOSError(errno.EACCES)
		return 0

	def chmod(self, path, mode):
		pass

	def chown(self, path, uid, gid):
		pass

	def getattr(self, path, fh=None):
		if debug:
			print "getattr  " + path 
		isFolder = False
		if len(path.split('/')) == 2:
			isFolder = True

		"""link_data = {
			"st_ctime" : 1456615173,
			"st_mtime" : 1456615173,
			"st_nlink" : 2,
			"st_mode" : 16893,
			"st_size" : 2,
			"st_gid" : 1000,
			"st_uid" : 1000,
			"st_atime" : time(),
		}"""


		folder_data = {
			"st_ctime" : 1456615173,
			"st_mtime" : 1456615173,
			"st_nlink" : 2,
#			"st_mode" : 16893,
			"st_mode" : 16895,
			"st_size" : 2,
			"st_gid" : 1000,
			"st_uid" : 1000,
			"st_atime" : time(),
		}

		
		full_path = self._full_path(path)
		try:
			st = os.lstat(full_path)
			print st
			rdata = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
		except: 
			pass
		#if os.path.isfile == True:
		#	return 
		if isFolder:
			for container in list(self.service.list_containers()):
				if container.name == path[1:]:
					return folder_data
		else:
			"""import config as config
			account_name = config.STORAGE_ACCOUNT_NAME
			account_key = config.STORAGE_ACCOUNT_KEY"""
			containername = path.split('/')[1]
			filename = path.split('/')[2]
			"""block_blob_service = BlockBlobService(account_name, account_key)
			if os.path.isfile(full_path) == False:
				fileSize = 1
			else:
				try:
					pass
					fileSize = os.path.getsize(full_path)
				except:
					fileSize = 1"""
			self.service = self.account.create_block_blob_service()
			file_data = {
				"st_ctime" : 1456615173,
				"st_mtime" : 1456615173,
				"st_nlink" : 1,
#				"st_mode" : 33188,
				"st_mode" : 33279,
				"st_size" : self.service.get_blob_properties(containername, filename).properties.content_length,
				"st_gid" : 1000,
				"st_uid" : 1000,
				"st_atime" : time(),
			}
			return file_data

		st = os.lstat(full_path)
		print st
		rdata = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
		return rdata

	def readdir(self, path, fh):
		if debug:
			print "readdir  " + path  
		
		full_path = self._full_path(path)

		dirents = ['.', '..']
		#if os.path.isdir(full_path):
		#	dirents.extend(os.listdir(full_path))
		for r in dirents:
			yield r
		containers = list(self.service.list_containers())
		#print('All containers in your account:')
		if path == "/":
			for container in containers:
				yield container.name
		else: 
			folder = path[1:]
			blobs = list(self.service.list_blobs(folder))
			for blob in blobs:
				yield blob.name
			

	def readlink(self, path):
		if debug:
			print "readlink" 

		pathname = os.readlink(self._full_path(path))
		if pathname.startswith("/"):
			# Path name is absolute, sanitize it.
			return os.path.relpath(pathname, self.root)
		else:
			return pathname

	def mknod(self, path, mode, dev):
		return os.mknod(self._full_path(path), mode, dev)

	def rmdir(self, path):
		if debug:
			print "rmdir  " + path[1:]
		deleted = self.service.delete_container(path[1:])
		return 0

	def mkdir(self, path, mode):
		"""
		Only valid in the top level of the mounted directory.
		Creates a container to serve as the folder 

		A container name must be a valid DNS name, conforming to the following 
		naming rules:
		1) Container names must start with a letter or number, and can contain
		   only letters, numbers, and the dash (-) character.
		2) Every dash (-) character must be immediately preceded and followed 
		   by a letter or number; consecutive dashes are not permitted in
		   container names.
		3) All letters in a container name must be lowercase.
		4) Container names must be from 3 through 63 characters long.

		30 second lease timeout on deleted directory.
		"""
		if debug:
			print "mkdir  " + path[1:]

		# TODO: validate input 
		self.service.create_container(path[1:])
		return 0

	def statfs(self, path):
		full_path = self._full_path(path)
		stv = os.statvfs(full_path)
		return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
			'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
			'f_frsize', 'f_namemax'))

	def unlink(self, path):
		return os.unlink(self._full_path(path))

	def symlink(self, name, target):
		return os.symlink(name, self._full_path(target))

	def rename(self, old, new):
		"""
		1) create new container
		2) stream contents of old container to new container
		3) delete old container
		"""
		# step 1 
		self.mkdir(new, 0777)

		# step 2
		# TODO: steam contents to new container
		"""import config as config
		account_name = config.STORAGE_ACCOUNT_NAME
		account_key = config.STORAGE_ACCOUNT_KEY
		block_blob_service = BlockBlobService(account_name, account_key)
		block_blob_service.get_blob_to_path(containername, filename, tempfilename)	
		block_blob_service.create_blob_from_path(new, filename, filename)"""	
				
	
		#step 3
		self.rmdir(old)

	def link(self, target, name):
		return os.link(self._full_path(target), self._full_path(name))

	def utimens(self, path, times=None):
		return os.utime(self._full_path(path), times)

	# File methods
	# ============

	def open(self, path, flags):
		if debug:
			print "open:    " + path
			print flags

		return 0#os.open(full_path, flags)

	def create(self, path, mode, fi=None):
		if debug:
			print "create:   " + path
		full_path = self._full_path(path)
		return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

	def read(self, path, length, offset, fh):
		if debug:
			print "read:	   " + path
			print "offset:  " 
			print offset
			print "length: "
			print length 
			print fh
		full_path = self._full_path(path)
		import config as config
		account_name = config.STORAGE_ACCOUNT_NAME
		account_key = config.STORAGE_ACCOUNT_KEY
		containername = path.split('/')[1]
		filename = path.split('/')[2]
		block_blob_service = BlockBlobService(account_name, account_key)
		try:
			print "get block blob" 
			if os.path.isdir(path.split('/')[1]) == False:
				os.mkdir(full_path.split('/')[0]+'/'+containername)
			if os.path.isfile(full_path) == False:
				block_blob_service.get_blob_to_path(containername, filename, full_path)
			else:
				print "get block blob" 
				os.remove(full_path)
				block_blob_service.get_blob_to_path(containername, filename, full_path)
		except:
			pass
		print "full path:   " + full_path 
		print os.path.isfile(full_path)		
		
		full_path = self._full_path(path)
		print full_path
		#os.lseek(fh, offset, os.SEEK_SET)
		#if os.path.isfile(full_path) == False:
		import config as config
		account_name = config.STORAGE_ACCOUNT_NAME
		account_key = config.STORAGE_ACCOUNT_KEY
		containername = path.split('/')[1]
		filename = path.split('/')[2]
		print filename
		block_blob_service = BlockBlobService(account_name, account_key)
		try:
			if os.path.isdir(path.split('/')[1]) == False:
				os.mkdir(full_path.split('/')[0]+'/'+containername)
			if os.path.isfile(full_path) == False:
				print "read block blob" 
				block_blob_service.get_blob_to_path(containername, filename, full_path)
			else:
				os.remove(full_path)
				block_blob_service.get_blob_to_path(containername, filename, full_path)
		except:
			pass
			
		fhn = os.open(full_path, 32768)
		os.lseek(fhn, offset, os.SEEK_SET)

		#return os.read(fh, length)
		return os.read(fhn, length)

	def write(self, path, buf, offset, fh):
		if debug:
			print "write:   " + path
		os.lseek(fh, offset, os.SEEK_SET)
		return os.write(fh, buf)

	def truncate(self, path, length, fh=None):
		print "truncate:   " + path
		full_path = self._full_path(path)
		with open(full_path, 'r+') as f:
			f.truncate(length)

	def flush(self, path, fh):
		print "flush:   " + path
		return os.fsync(fh)

	def release(self, path, fh):
		print "release:   " + path
		return os.close(fh)

	def fsync(self, path, fdatasync, fh):
		print "fsync:   " + path
		return self.flush(path, fh)


def main(mountpoint, root):
	FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
	main(sys.argv[2], sys.argv[1])
