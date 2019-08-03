"""
Author:
16 Bit Inc.

For any questions and comments regarding usage and technical details,
please send correspondences to Daniel Eftekhari at daniel@16bit.ai

Program name:
dcmAnonymizerV02.py

Software requirements:
1. Anaconda Distribution Python version 2.7.*
Download Anaconda at https://www.anaconda.com/download/
2. After installing Anaconda, add /PATH/TO/ANACONDA and /PATH/TO/ANACONDA/SCRIPTS to your system PATH.
3. Please make sure pydicom has been installed prior to running this program.
This can be done by entering <conda install -c conda-forge pydicom> in the command line/prompt.
4. There are several image handler packages which may or may not be needed depending on your dicom transfer syntaxes.
These packages are imported by default. Only <gdcm> and <jpeg_ls> need to be installed manually (assuming Anaconda Distribution is being used).
This can be done by running <conda install -c conda-forge gdcm> to install gdcm>,
and cloning the CharPyLs repository from https://github.com/Who8MyLunch/CharPyLS and running <pip install .> from inside the CharPyLs directory.
See https://pydicom.github.io/pydicom/stable/image_data_handlers.html for specifications on which handlers may be needed for your dicom files.

Usage:
python dcmAnonymizerV02.py -d <input directory> -o <output directory> -l <linking log directory> -g <s/m/n> (group into subfolders by either studyID (s) or MRN (m), or do not group into subfolders at all (n))
Example usage where the output dicoms are grouped into subfolders by anonymized studyID:
python dcmAnonymizerV02.py -d ./data -o ./anondata -l ./linklog -g s

Program input:
1. Top-level directory containing all dicoms, either directly within the directory, or in subdirectories.

Notes:
1. Please make sure that the linking log folder path already exists on the local drive.
For the same dataset, this path must be consistent across different runs of the program.
2. If the program terminates because extra disk space is needed to write dicoms to the output folder,
run the program again as many times as needed, each time with a new output folder containing additional disk space.

Program output:
1. For each dicom in the input directory (recursive for subdirectories), if it doesn't already exist, the program writes an anonymized version to the desired output directory.
The output (anonymized) dicoms are grouped into subfolders by studyID/MRN, or are not grouped into subfolders at all.
2. Generates or updates existing link log files. These are used to determine whether a dicom has already been anonymized or not.
"""

from __future__ import print_function

import sys
import os
import logging
import psutil

import time
import datetime

import config
import constructDicom
from utils import load_json, save_json, load_link_log, find_max

import pydicom

DICOM_FIELDS = ('PatientID', 'AccessionNumber', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID')
IDENTIFIER_FIELDS = ('mrn', 'accession', 'studyID', 'seriesID', 'sopID')
MAX_FIELDS = ('max_mrn', 'max_accession', 'max_studyID', 'max_seriesID', 'max_sopID')
LINK_LOG_FIELDS = ('link_mrn_log', 'link_accession_log', 'link_study_log', 'link_series_log', 'link_sop_log', 'link_master_log')

RESERVE_OUTPUT_SPACE = 50*10**6


def get_dicoms(dcm_directory):
    partition = {}
    if os.path.isdir(dcm_directory):
        # Walks through directory, and returns a dictionary with
        # keys as root folder paths and
        # values as file paths and total directory size.
        print("Getting dicoms in", dcm_directory)
        logger.info("Getting dicoms in {}".format(dcm_directory))
        for root, dirs, files in os.walk(dcm_directory):
            partition[root] = {'queue': [], 'size': 0.0}
            for name in files:
                is_dicom = False
                if name.endswith((".dcm", ".dicom")):
                    is_dicom = True
                else:
                    try:
                        _ = pydicom.dcmread(os.path.join(root, name))
                        is_dicom = True
                    except:
                        pass
                if is_dicom:
                    partition[root]['queue'].append(os.path.join(root, name))
                    partition[root]['size'] += os.stat(os.path.join(root, name)).st_size
        return partition
    else:
        print("DICOM directory does not exist - check the path")
        logger.error("DICOM directory does not exist - check the path")
    return partition


def anonymize_dicoms(link_log_path, partition, out_dir, grouping, link_dict):
    # Determine where the incrementer stopped in previous runs of the program.
    # Important for creating new identifiers for newly encountered cases.
    max_values = {}
    for i_iter in range(len(MAX_FIELDS)):
        max_values[MAX_FIELDS[i_iter]] = find_max(link_dict[LINK_LOG_FIELDS[i_iter]])

    directories = partition.keys()
    for directory in directories:
        # Check space limitation. Terminate program if space left is too small.
        free_space = float(psutil.disk_usage(out_dir).free)
        if partition[directory]['size'] > free_space or free_space < RESERVE_OUTPUT_SPACE:
            print('Ran out of space to write files.')
            logger.warning('Ran out of space to write files.')
            break

        for f in partition[directory]['queue']:
            ds = pydicom.dcmread(f)

            # Check if requisite tags exist
            is_valid_dicom_image = True
            for dicom_field in DICOM_FIELDS:
                if dicom_field not in ds:
                    logger.warning("WARNING - file: {} | {} not in DICOM tags".format(str(f), dicom_field))
                    is_valid_dicom_image = False

            if is_valid_dicom_image:
                values = (ds.PatientID, ds.AccessionNumber, ds.StudyInstanceUID, ds.SeriesInstanceUID, ds.SOPInstanceUID)
                anon_values = {identifier: None for identifier in IDENTIFIER_FIELDS}

                for i_iter in range(len(DICOM_FIELDS)):
                    # Create a unique link between dicom info and anonymous keys to be stored.
                    if str(values[i_iter]) in link_dict[LINK_LOG_FIELDS[i_iter]]:
                        anon_values[IDENTIFIER_FIELDS[i_iter]] = link_dict[LINK_LOG_FIELDS[i_iter]][str(values[i_iter])]
                    else:
                        max_values[MAX_FIELDS[i_iter]] += 1
                        anon_values[IDENTIFIER_FIELDS[i_iter]] = max_values[MAX_FIELDS[i_iter]]
                        link_dict[LINK_LOG_FIELDS[i_iter]][str(values[i_iter])] = anon_values[IDENTIFIER_FIELDS[i_iter]]

                # If combination of keys already exists in the cache, skip the current dicom.
                dicom_tuple = tuple(anon_values[IDENTIFIER_FIELDS[i_iter]] for i_iter in range(len(IDENTIFIER_FIELDS)))
                if str(dicom_tuple) in link_dict[LINK_LOG_FIELDS[-1]]:
                    link_dict[LINK_LOG_FIELDS[-1]][str(dicom_tuple)] += 1
                    print('mrn-accession-studyID-seriesID-sopID tuple has already been anonymized.')
                    logger.info('mrn-accession-studyID-seriesID-sopID tuple has already been anonymized.')
                else:
                    try:
                        constructDicom.write_dicom(ds, anon_values, out_dir, grouping)
                        link_dict[LINK_LOG_FIELDS[-1]][str(dicom_tuple)] = 1
                    except Exception as error:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        logger.warning('WARNING - file: {} | message: {} {} {} . This warning is for case {} with anon_values {} .'
                            .format(str(f), str(error), str(exc_type), str(exc_tb.tb_lineno), str(values), str(anon_values)))

        # Remove directory's dicoms from further consideration.
        del partition[directory]

    # Save cache of already-visited patients.
    for i_iter in range(len(LINK_LOG_FIELDS)):
         save_json(link_dict[LINK_LOG_FIELDS[i_iter]], os.path.join(link_log_path, "{}.json".format(LINK_LOG_FIELDS[i_iter])))

    # Update partition.
    save_json(partition, os.path.join(link_log_path, 'partition.json'))


if __name__ == "__main__":
    start_time = str(datetime.datetime.now())

    if not os.path.exists(os.path.join(os.getcwd(), 'stdout')):
        os.makedirs(os.path.join(os.getcwd(), 'stdout'))
    sys.stdout = open(os.path.join(os.getcwd(), 'stdout', 'stdout_{}'.format(''.join(start_time.split(':')))), 'w')

    # Parse command line arguments.
    args = config.parse_args()
    dcm_dir = args.dcmdir
    out_dir = args.outdir
    link_log = args.linklog
    grouping = args.group

    # Log at WARNING level.
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter(fmt='%(asctime)s:%(levelname)s:%(lineno)d:%(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    handler = logging.FileHandler(os.path.join(link_log, 'dcm_anonymize_{}.log'.format(''.join(start_time.split(':')))))
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print(dcm_dir, out_dir, link_log, grouping)
    logger.info(dcm_dir, out_dir, link_log, grouping)

    # Create output directory, if it doesn't already exist.
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Load partition, if it exists.
    try:
        partition = load_json(os.path.join(link_log, 'partition.json'))
        print('Loading existing partition.')
        logger.info('Loading existing partition.')
    except:
        partition = {}

    # Load cache of cases already analyzed, otherwise instantiate new caches
    link_dict = {}
    for i_iter in range(len(LINK_LOG_FIELDS)):
        link_dict[LINK_LOG_FIELDS[i_iter]] = load_link_log(logger, link_log, "{}.json".format(LINK_LOG_FIELDS[i_iter]), "Loading existing {}.".format(LINK_LOG_FIELDS[i_iter]))

    # Load and anonymize dicoms.
    try:
        if not partition:
            start_time_get_dicoms = time.time()
            partition = get_dicoms(dcm_dir)
            end_time_get_dicoms = time.time()
            print("--- Process get_dicoms took %s seconds to execute ---" % round((end_time_get_dicoms - start_time_get_dicoms), 2))
            save_json(partition, os.path.join(link_log, 'partition.json'))

        start_time_anonymize_dicoms = time.time()
        anonymize_dicoms(link_log, partition, out_dir, grouping, link_dict)
        end_time_anonymize_dicoms = time.time()
        print("--- Process anonymize_dicoms took %s seconds to execute ---" % round((end_time_anonymize_dicoms - start_time_anonymize_dicoms), 2))
    except ValueError:
        print("DICOM file list could not be loaded.")
        logger.error("DICOM file list could not be loaded.")

    print("Anonymization Complete!")
    logger.info("Anonymization Complete!")
