"""
@name verify-azure.py
@author mbartoli 

Sample tests for Azure blob storage using the Microsoft Azure Python SDK. 
Tests modified from https://github.com/Azure/azure-storage-python

"""
from azure.storage import CloudStorageAccount
from tests import (
	BlobSasSamples,
	ContainerSamples,
	BlockBlobSamples,
	AppendBlobSamples,
	PageBlobSamples,
)

class SampleTest():
	def __init__(self):
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

	def test_container_samples(self):
		container = ContainerSamples(self.account)
		container.run_all_samples()

	def test_block_blob_samples(self):
		blob = BlockBlobSamples(self.account)
		blob.run_all_samples()

	def test_append_blob_samples(self):
		blob = AppendBlobSamples(self.account)
		blob.run_all_samples()

	def test_page_blob_samples(self):
		blob = PageBlobSamples(self.account)
		blob.run_all_samples()

	def list_containers(self):
		self.service = self.account.create_block_blob_service()
		containers = list(self.service.list_containers())
		print('All containers in your account:')
		for container in containers:
			print(container.name)     
	
	def list_all_blobs_in_all_containers(self):
		self.service = self.account.create_block_blob_service()
		containers = list(self.service.list_containers())
		print('Full list:')
		for container in containers:
			print(container.name+':')		
			blobs = list(self.service.list_blobs(container.name))
			for blob in blobs:
				print(blob.name)
			print('')

def main():
	tests = SampleTest()
	#tests.test_container_samples()	
	#tests.test_append_blob_samples()
	#tests.test_page_blob_samples()
	#tests.list_containers()
	tests.list_all_blobs_in_all_containers()

if __name__ == '__main__':
	main() 
