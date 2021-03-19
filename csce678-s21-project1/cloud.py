from basic_defs import cloud_storage, NAS

import os
import sys
import boto3
import hashlib
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
from botocore.exceptions import ClientError
from google.cloud import storage
from google.cloud.exceptions import NotFound


class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = "" #hidden for git
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = "" #hidden for git
        # TODO: Fill in the bucket name
        self.bucket_name = "" #hidden for git
        self.aws_s3_client = boto3.client('s3', aws_access_key_id=self.access_key_id,
                                 aws_secret_access_key=self.access_secret_key)
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from boto3
    #     boto3.session.Session:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    #     boto3.resources:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    #     boto3.s3.Bucket:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    #     boto3.s3.Object:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
    def list_blocks(self):
        client_contents = self.aws_s3_client.list_objects(Bucket=self.bucket_name)["Contents"]
        block_list = []
        if len(client_contents)>0:
            for item in client_contents:
                    if item['Key'].isdigit():
                        block_list.append(int(item['Key']))
                    else:
                        block_list.append(item['Key'])

        return block_list
        
    def read_block(self, offset):
        try:
            item = self.aws_s3_client.get_object(Bucket=self.bucket_name, Key=str(offset))
            return bytearray(item['Body'].read())
        except ClientError as e:
            return bytearray([])
            
    def write_block(self, block, offset):
        try:
            response = self.aws_s3_client.put_object(Body=bytearray(block), Bucket=self.bucket_name, Key=str(offset))
        except ClientError as e:
            pass
        
    def delete_block(self, offset):
        try:
            item = self.aws_s3_client.delete_object(Bucket=self.bucket_name, Key=str(offset))
        except ClientError as e:
            pass

        
class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = "" #hidden for git
        # TODO: Fill in the Azure connection string
        self.conn_str = "" #hidden for git
        # TODO: Fill in the account name
        self.account_name = "" #hidden for git
        # TODO: Fill in the container name
        self.container_name = "" #hidden for git
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python
    def list_blocks(self):
        block_list = []
        try:
            for blob in self.container_client.list_blobs():
                if blob.name.isdigit():
                    block_list.append(int(blob.name))
                else:
                    block_list.append(blob.name)
        except ResourceNotFoundError:
            print("Containers does not exist on Azure Storage.")
        return block_list

    def read_block(self, offset):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
            data = blob_client.download_blob().readall()
            return bytearray(data)
        except ResourceNotFoundError:
            return bytearray([])

    def write_block(self, block, offset):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
        ret_dict = blob_client.upload_blob(bytearray(block),
                                           blob_type="BlockBlob",
                                           overwrite=True) 


    def delete_block(self, offset):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
            blob_client.delete_blob()  
        except ResourceNotFoundError:
            pass
    
    
class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = "gcp-credential.json"
        # TODO: Fill in the container name
        self.bucket_name = "" #hidden for git
        self.gcp_client = storage.Client.from_service_account_json(self.credential_file)
        self.gcp_bucket = self.gcp_client.get_bucket(self.bucket_name)
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from google.cloud.storage
    #    storage.client.Client:
    #        https://googleapis.dev/python/storage/latest/client.html
    #    storage.bucket.Bucket:
    #        https://googleapis.dev/python/storage/latest/buckets.html
    #    storage.blob.Blob:
    #        https://googleapis.dev/python/storage/latest/blobs.html

    def list_blocks(self):
        blobs = list(self.gcp_client.list_blobs(self.gcp_bucket))
        blob_list = []
        for blob in blobs:
            if blob.name.isdigit():
                blob_list.append(int(blob.name))
            else:
                 blob_list.append(blob.name)
        return blob_list
        
    def read_block(self, offset):
        try:
            blob = storage.Blob(str(offset), self.gcp_bucket)
            block = blob.download_as_string()
            return bytearray(block)
        except NotFound:
            return bytearray([])
    
    def write_block(self, block, offset):
        blob = storage.Blob(str(offset), self.gcp_bucket)
        block = block if type(block) == str else str(block)
        blob.upload_from_string(block) 


    def delete_block(self, offset):
        try:
            blob = storage.Blob(str(offset), self.gcp_bucket)
            blob.delete()
        except NotFound:
            pass

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]

    # Implement the abstract functions from NAS
        self.fd_dict = {}
        self.max_file_descriptors = 256
        self.block_size = 4096
    
    def open(self, filename):
        for i in range(self.max_file_descriptors):
            if i not in self.fd_dict:
                self.fd_dict[i] = filename
                return i
    
    def read(self, fd, len, offset):
        print("READ: ", fd, len, offset)
        filename = ""
        if fd in self.fd_dict:
            filename = self.fd_dict[fd]
        else:
            return bytearray([])
        initial_block = offset//self.block_size
        initial_data_offset = offset%self.block_size
        last_block = (offset+len-1)//self.block_size
        last_data_offset = (offset+len-1)%self.block_size
        result = ""
        for i in range(initial_block, last_block+1):
            unique_identifier = filename+str(i*4096)
            cloud_provider, block_offset = self.get_cloud_location(unique_identifier)
            block = str(cloud_provider[0].read_block(block_offset))
            start_pointer = initial_data_offset if i == initial_block else 0
            end_pointer = last_data_offset + 1 if i == last_block else self.block_size
            block = block[start_pointer:end_pointer]
            result = result + block;
        return bytearray(result)
    
    def get_cloud_location(self, unique_identifier):
        hash_value = hash(unique_identifier)
        mod_val = hash_value%3
        store_on_clouds = [x for i,x in enumerate(self.backends) if i!=mod_val]
        m = hashlib.sha256()
        m.update(bytearray(unique_identifier))
        block_offset = m.hexdigest()
        return store_on_clouds, block_offset
        
    def write(self, fd, data, offset):
        
        filename = self.fd_dict[fd]
        
        initial_block = offset//self.block_size
        initial_data_offset = offset%self.block_size 
        
        last_block = (offset+len(data)-1)//self.block_size
        last_data_offset = (offset+len(data)-1)%self.block_size
        
        for i in range(initial_block, last_block+1):
            unique_identifier = filename+str(i*4096)
            cloud_provider, block_offset = self.get_cloud_location(unique_identifier)
            start_pointer = initial_data_offset if i == initial_block else 0
            end_pointer = last_data_offset + 1 if i == last_block else self.block_size
            req_length = end_pointer - start_pointer
            new_block = data[:req_length]
            existing_block = str(cloud_provider[0].read_block(block_offset))
            if existing_block:
    		    start_trim = existing_block[:start_pointer] if i==initial_block else ""
    		    end_trim = existing_block[end_pointer:] if i== last_block else ""
    		    new_block = start_trim + new_block + end_trim
    		    
    	    for cloud in cloud_provider:
    		    cloud.write_block(new_block, block_offset)
            data = data[req_length:] #check this
    
    def close(self, fd):
        del self.fd_dict[fd]

    def delete(self, filename):
        i = 0
        unique_identifier = filename + str(i * 4096)
        cloud_provider, block_offset = self.get_cloud_location(unique_identifier)
        stuff = str(cloud_provider[0].read_block(block_offset))
        if (stuff):
            flag=True
        else:
            flag=False
        while flag:
            for cloud in cloud_provider:
                cloud.delete_block(block_offset)
            i = i+1
            key = filename + str(i * 4096)
            cloud_provider, block_offset = self.get_cloud_location(key)
            stuff = str(cloud_provider[0].read_block(block_offset))
            if not stuff:
                flag=False            
    
