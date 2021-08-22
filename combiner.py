import numpy as np
import h5py
import tarfile
import os
from collections import Counter


class Combiner:

    def __init__(self, source_path, output_path, loc_id):
        self.loc_id = loc_id
        self.output_path = output_path
        file_path = os.path.join(source_path, loc_id)
        self.tar_files = self._get_files(file_path)

    def _get_files(self, file_path):
        tar_files = [os.path.join(file_path, f)
                     for f in os.listdir(file_path) if 'tar' in f]
        return tar_files

    def _read_label_data(self):
        label_dict = {}

        for tar_file in self.tar_files:
            tar_content = tarfile.open(tar_file, "r:gz")
            members = tar_content.getmembers()
            h5file = tar_content.extractfile(members[1].name)
            f = h5py.File(h5file, 'r')
            keys = list(f.keys())
            for key in keys:
                if key in label_dict:
                    label_dict[key].append(np.array(f[key]))
                else:
                    label_dict[key] = []
                    label_dict[key].append(np.array(f[key]))

            f.close()
            tar_content.close()
        return label_dict

    @staticmethod
    def _get_most_common(lst):
        data = Counter(lst)
        return data.most_common(1)[0][0]

    @staticmethod
    def _save_h5(result_dict, output_path):

        hf_result = h5py.File(output_path, 'w')
        for k, v in result_dict.items():
            hf_result.create_dataset(k, data=v)
        hf_result.close()

    def combine(self):
        label_dict = self._read_label_data()
        result_dict = {}
        for k, v in label_dict.items():
            stacked_label = np.stack(v, axis=2)
            stacked_label = stacked_label.astype(int)
            x = stacked_label.shape[0]
            y = stacked_label.shape[1]
            fixed_result = np.zeros((x, y))
            for i in range(0, x):
                for j in range(0, y):
                    local_max = self._get_most_common(stacked_label[i, j])
                    fixed_result[i, j] = local_max
            result_dict[k] = fixed_result

        output_path = os.path.join(self.output_path, '{0}_combined.h5'.format(self.loc_id))
        self._save_h5(result_dict, output_path)
        return output_path
