import numpy as np
import h5py
import tarfile
from collections import Counter


class Combiner:

    def __init__(self, storage_helper, loc_id):
        self.storage_helper = storage_helper
        self.tar_files = self.storage_helper.get_tar_files(loc_id)
        self.loc_id = loc_id

    def _read_label_data(self):
        """ Read all tar.gz files and save all label data in a dictionary

        Returns:
            label_dict: a dictionary, key is the class key,
                        and value is a list of all label data from multiple tar.gz files.
                        for typical 896*896 case, value is a list of nparrays with shape of (896,896)
        """
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
        """
        Given a list, find the value that occurs the most.
        """
        data = Counter(lst)
        return data.most_common(1)[0][0]

    def combine(self):
        """ Combine label data from multiple label files.
        For each pixel, select the majority as the final value. for example:
        labels of 5 jobs for pixel [0,0] are [-1,1,0,1,1], the final label for [0,0] is 1.
        """
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

        output_path = self.storage_helper.save_h5_file(result_dict, self.loc_id)
        return output_path
