import os
import h5py
import boto3


class StorageHelper:
    """
    Helper class for interacting with storage. support both local storage and s3.
    """

    def __init__(self, source_path, output_path):
        """
        storage_type is determined based on source_path format
        """
        self.storage_type = 's3' if 's3' in source_path else 'local'
        self.source_path = source_path
        self.output_path = output_path

    def get_tar_files(self, loc_id):
        """
        Get tar.gz files of a given location id.

        Args:
            loc_id: the location id.
        Returns:
            tar_files: a list of links on storage.
        """
        if self.storage_type == 's3':
            conn = boto3.client('s3')
            bucket_name = self.source_path.split('//')[1].split('/')[0]
            prefix = self.source_path.split(bucket_name+'/')[1]+'/'+loc_id
            s3_result = conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
            if 'Contents' not in s3_result:
                # print(s3_result)
                return []

            tar_files = []
            for key in s3_result['Contents']:
                tar_files.append(key['Key'])

        else:
            file_path = os.path.join(self.source_path, loc_id)
            tar_files = [os.path.join(file_path, f)
                         for f in os.listdir(file_path) if 'tar' in f]
        return tar_files

    def save_h5_file(self, result_dict, loc_id):
        """
        Get tar.gz files of a given location id.

        Args:
            result_dict: a dictionary of class and corresponding combined labels.
                        labels are in data type of nparray.
            loc_id: the location id.
        Returns:
            tar_files is a list of links on storage.
        """
        if self.storage_type == 's3':
            file_path = '{0}/{1}_combined.h5'.format(
                self.output_path,
                loc_id
            )
            hf_result = h5py.File(file_path, 'w')
            for k, v in result_dict.items():
                hf_result.create_dataset(k, data=v)
            hf_result.close()
        else:
            file_path = os.path.join(self.output_path, '{0}_combined.h5'.format(loc_id))
            hf_result = h5py.File(file_path, 'w')
            for k, v in result_dict.items():
                hf_result.create_dataset(k, data=v)
            hf_result.close()
        return file_path

