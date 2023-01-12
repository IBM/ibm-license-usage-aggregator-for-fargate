#
# Copyright 2023 IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#

import filecmp
import os
import shutil
import sys
from pathlib import Path

file = Path(__file__).resolve()
os.chdir(sys.path[0])
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

import pytest
import scripts.IBM_license_usage_aggregator_for_fargate as Aggr

try:
    sys.path.remove(str(parent))
except ValueError:
    pass


class DirCmp(filecmp.dircmp):
    """
    Compare the content of dir1 and dir2. In contrast with filecmp.DirCmp, this
    subclass compares the content of files with the same path.
    """
    def phase3(self):
        """
        Find out differences between common files.
        Ensure we are using content comparison with shallow=False.
        """
        comp = filecmp.cmpfiles(self.left, self.right, self.common_files, shallow=False)
        self.same_files, self.diff_files, self.funny_files = comp


def is_same(dir1, dir2):
    """
    Compare two directory trees content.
    Return False if they differ
    """
    compared = DirCmp(dir1, dir2)
    if compared.left_only or compared.right_only or compared.diff_files or compared.funny_files:
        return False
    for subdir in compared.common_dirs:
        if not is_same(os.path.join(dir1, subdir), os.path.join(dir2, subdir)):
            return False
    return True


@pytest.fixture(autouse=True)
def mkdir():
    try:
        shutil.rmtree("test_files/output_test")
    except FileNotFoundError:
        pass
    
    os.mkdir("test_files/output_test")


def test_compare_results_equal():
    Aggr.main(["test_files/input", "test_files/output_test"])
    assert is_same("test_files/output_test", "test_files/output")


def test_compare_results_different():
    """
    Appends a string to one of the results to verify 
    that previous test works correctly
    """
    Aggr.main(["test_files/input", "test_files/output_test"])
    f = open("test_files/output_test/products_daily_2022-02-22_2022-02-24_\
arn_aws_ecs_eu-central-1_675801125365_cluster_testCluster2.csv", "a")
    f.write("test")
    f.close()
    assert not is_same("test_files/output_test", "test_files/output")
