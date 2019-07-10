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
This can be done by running <conda install -c conda-forge gdcm> to install gdcm, and cloning the CharPyLs repository from https://github.com/Who8MyLunch/CharPyLS and running <pip install .> from inside the CharPyLs directory.
See https://pydicom.github.io/pydicom/dev/image_data_handlers.html for specifications on which handlers may be needed
for your dicom files.

Usage:
python dcmAnonymizerV02.py -d <input directory> -o <output directory> -l <linking log directory> -s <Space available for output directory (in GB)> -g <s/m/n> (group into subfolders by either studyID or MRN, or do not group into subfolders at all)
Example usage, where the output directory has 4000 GB (or 4 Terabytes) of space, and the output dicoms are grouped into subfolders by anonymized studyID:
python dcmAnonymizerV02.py -d D:\16bitproject\16bit -o D:\16bitproject\16bitanon -l D:\16bitproject\16bitlog -s 4000 -g s

Program input:
1. Top-level directory containing all dicoms, either directly within the directory, or in subdirectories.

Notes:
1. Please make sure that the linking log folder path already exists on the local drive.
For the same dataset, this path must be consistent across different runs of the program.
This path should not be the path of a portable external drive.
2. If the dataset is larger than the specified space, run the program as many times as needed, each time specifying the space available in the output directory.
3. This version of the program uses parallelism (multiple processors) to accelerate anonymization.
See https://docs.python.org/2/library/multiprocessing.html for details.

Program output:
1. For each dicom in the input directory (recursive for subdirectories), if it doesn't already exist, the program writes an anonymized version to the desired output directory.
The output (anonymized) dicoms are grouped into subfolders by studyID/MRN, or are not grouped into subfolders at all.
2. Generates or updates existing link log files. These are used to determine whether a dicom has already been anonymized or not.
"""

from __future__ import print_function

import sys
import os
import logging
import argparse
import time
from datetime import datetime as dt
import json

# Image handlers
IMPORT_ERROR_MESSAGE = 'could not be imported. This may cause issues, depending on the transfer syntaxes of the dicom files.'
try:
    import numpy as np
except ImportError:
    print('Python package numpy', IMPORT_ERROR_MESSAGE)
try:
    import PIL
except ImportError:
    print('Python package PIL', IMPORT_ERROR_MESSAGE)
try:
    import jpeg_ls
except ImportError:
    print('Python package jpeg_ls', IMPORT_ERROR_MESSAGE)
try:
    import gdcm
except ImportError:
    print('Python package gdcm', IMPORT_ERROR_MESSAGE)

import pydicom
from pydicom.dataset import Dataset, FileDataset
from pydicom.tag import Tag

import multiprocessing as mp

DICOM_FIELDS = ('PatientID', 'AccessionNumber', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID')
IDENTIFIER_FIELDS = ('mrn', 'accession', 'studyID', 'seriesID', 'sopID')
MAX_FIELDS = ('max_mrn', 'max_accession', 'max_studyID', 'max_seriesID', 'max_sopID')
LINK_LOG_FIELDS = ('link_mrn_log', 'link_accession_log', 'link_study_log', 'link_series_log', 'link_sop_log', 'link_master_log')


def load_json(file_name):
    with open(file_name) as data_file:
        data = json.load(data_file)
    return data


def save_json(data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(obj=data, fp=outfile, sort_keys=True, indent=4, separators=(',', ': '))


def load_link_log(link_log_path, file_name, message):
    if link_log_path is not None and os.path.exists(os.path.join(link_log_path, file_name)):
        link_dict = load_json(os.path.join(link_log_path, file_name))
    else:
        link_dict = {}
        print(message)
        logger.info(message)
    return link_dict


def parse_args():
    parser = argparse.ArgumentParser(description="Anonymizes DICOM directory")
    parser.add_argument("-d", "--dcmdir", type=str, help="Input DICOM directory path", required=True)
    parser.add_argument("-o", "--outdir", type=str, help="Output DICOM directory path", required=True)
    parser.add_argument("-l", "--linklog", type=str, help="Linking log directory", required=True)
    parser.add_argument("-s", "--space", type=str, help="Space available for output directory (in GB)", required=True)
    parser.add_argument("-g", "--group", type=str, help="Group output dicoms into subfolders by anonymized studyID (s) or MRN (m), or do not group into subfolders at all (n)", required=True)
    args = parser.parse_args()
    return args


def calculate_age(study_date, dob):
    if study_date and dob:
        d1 = dt.strptime(study_date, "%Y%m%d")
        d2 = dt.strptime(dob, "%Y%m%d")
        a = abs((d1 - d2).days)/365
        # format with leading 0
        age = str('%03d' % a)+'Y'
    else:
        age = ''
    return age


def clean_string(string):
    chars_to_remove = ['/', '(', ')', '^', '[', ']', ';', ':']
    for char in chars_to_remove:
        string = string.replace(char, "")
    string = string.replace(" ", "-")
    return string


def get_dicoms_mp(partition_queue, root, dirs, files):
    partition_queue[root] = {'queue': [], 'size': 0.0}
    for name in files:
        is_dicom = False
        if name.endswith((".dcm", ".dicom")):
            is_dicom = True
        else:
            try:
                _ = pydicom.read_file(os.path.join(root, name))
                is_dicom = True
            except:
                pass
        if is_dicom:
            content = partition_queue[root]

            content['queue'].append(os.path.join(root, name))
            content['size'] += os.stat(os.path.join(root, name)).st_size

            partition_queue[root] = content


def get_dicoms(dcm_directory):
    if os.path.isdir(dcm_directory):
        # Walks through directory and returns a dictionary of each root and its files and root sizes.
        print("Getting dicoms in", dcm_directory)
        logger.info("Getting dicoms in {}".format(dcm_directory))

        manager = mp.Manager()
        partition_queue = manager.dict()

        n_cores = mp.cpu_count()
        pool = mp.Pool(n_cores)

        for root, dirs, files in os.walk(dcm_directory):
            pool.apply_async(get_dicoms_mp, args=(partition_queue, root, dirs, files))
        pool.close()
        pool.join()

        partition_queue = partition_queue.copy()

        return partition_queue
    else:
        print("DICOM directory does not exist - ensure path exists")
        logger.error("DICOM directory does not exist - ensure path exists")
        return None


def find_max(link_dict):
    if link_dict:
        max_value = max(link_dict.values())
    else:
        max_value = 0
    return max_value


def calculate_progress(count, file_num, start_time):
    end_cycle_time = time.time()
    time_left = (end_cycle_time - start_time)/count*(file_num - count)/60.0
    print("---------------" + str(round((count/float(file_num))*100.0, 1)) + "% Complete (" + str(count) + "/" + str(file_num) + " | " + str(round(time_left, 1)) + " minutes remaining)----------------------")


def write_dicom(ods, keys, keys_file_meta, anon_values, out_dir, grouping):
    # Write dicom to file
    file_meta = Dataset()

    # Initializing all file_meta fields to empty - not needed.
    # for key in keys_file_meta:
    #     setattr(file_meta, key, "")

    file_meta.MediaStorageSOPClassUID = 'Secondary Capture Image Storage'
    file_meta.MediaStorageSOPInstanceUID = str(anon_values['sopID'])
    file_meta.ImplementationClassUID = '0.0'
    file_meta.TransferSyntaxUID = ods.file_meta.TransferSyntaxUID if "TransferSyntaxUID" in ods.file_meta else "0.0"
    ds = FileDataset(anon_values['studyID'], {}, file_meta=file_meta, preamble="\0"*128)

    # Initializing all dicom fields to empty - not needed.
    # for key in keys:
    #     setattr(ds, key, "")

    ds.Modality = ods.Modality if "Modality" in ods else ""
    ds.StudyDate = ods.StudyDate if "StudyDate" in ods else ""
    ds.StudyTime = ods.StudyTime if "StudyTime" in ods else ""
    ds.StudyInstanceUID = str(anon_values['studyID'])
    ds.SeriesInstanceUID = str(anon_values['seriesID'])
    ds.SOPInstanceUID = str(anon_values['sopID'])
    ds.SOPClassUID = 'Secondary Capture Image Storage'
    ds.SecondaryCaptureDeviceManufctur = 'Python 2.7'

    # These are the necessary imaging components of the FileDataset object.
    ds.AccessionNumber = str(anon_values['accession'])
    ds.PatientID = str(anon_values['mrn'])
    ds.StudyID = str(anon_values['studyID'])
    ds.PatientName = str(anon_values['studyID'])
    ds.SpecificCharacterSet = ods.SpecificCharacterSet if "SpecificCharacterSet" in ods else ""
    ds.ReferringPhysicianName = ""
    ds.PatientOrientation = ods.PatientOrientation if "PatientOrientation" in ods else ""
    ds.PatientBirthTime = "000000.000000"
    ds.PatientBirthDate = "00000000"
    ds.PatientAge = calculate_age(ods.StudyDate, ods.PatientBirthDate) if "StudyDate" in ods and "PatientBirthDate" in ods else ""
    ds.PatientSex = ods.PatientSex if "PatientSex" in ods else ""
    ds.StudyDescription = ods.StudyDescription if "StudyDescription" in ods else ""
    ds.SeriesDescription = ods.SeriesDescription if "SeriesDescription" in ods else ""
    ds.ViewPosition = ods.ViewPosition if "ViewPosition" in ods else ""
    ds.InstanceNumber = ods.InstanceNumber if "InstanceNumber" in ods else ""
    ds.SeriesNumber = ods.SeriesNumber if "SeriesNumber" in ods else ""
    ds.SamplesPerPixel = ods.SamplesPerPixel if "SamplesPerPixel" in ods else ""
    ds.PhotometricInterpretation = ods.PhotometricInterpretation if "PhotometricInterpretation" in ods else ""
    ds.PixelRepresentation = ods.PixelRepresentation if "PixelRepresentation" in ods else ""
    ds.HighBit = ods.HighBit if "HighBit" in ods else ""
    ds.BitsStored = ods.BitsStored if "BitsStored" in ods else ""
    ds.BitsAllocated = ods.BitsAllocated if "BitsAllocated" in ods else ""
    ds.Columns = ods.Columns if "Columns" in ods else ""
    ds.Rows = ods.Rows if "Rows" in ods else ""
    ds.ImagerPixelSpacing = ods.ImagerPixelSpacing if "ImagerPixelSpacing" in ods else ""
    # ds.WindowCenter = ods.WindowCenter if "WindowCenter" in ods else ""
    # ds.WindowWidth = ods.WindowWidth if "WindowWidth" in ods else ""
    # ds.PixelData = ods.PixelData if "PixelData" in ods else ""
    # ds.PixelData = ods.pixel_array
    ds.PixelData = ods.pixel_array.tobytes()
    ds.PresentationLUTShape = ods.PresentationLUTShape if "PresentationLUTShape" in ods else ""

    ds.KVP = ods.KVP if "KVP" in ods else ""
    ds.XRayTubeCurrent = ods.XRayTubeCurrent if "XRayTubeCurrent" in ods else ""
    ds.ExposureTime = ods.ExposureTime if "ExposureTime" in ods else ""
    ds.Exposure = ods.Exposure if "Exposure" in ods else ""
    ds.FocalSpots = ods.FocalSpots if "FocalSpots" in ods else ""
    ds.AnodeTargetMaterial = ods.AnodeTargetMaterial if "AnodeTargetMaterial" in ods else ""
    ds.BodyPartThickness = ods.BodyPartThickness if "BodyPartThickness" in ods else ""
    ds.CompressionForce = ods.CompressionForce if "CompressionForce" in ods else ""
    ds.PaddleDescription = ods.PaddleDescription if "PaddleDescription" in ods else ""
    ds.ExposureControlMode = ods.ExposureControlMode if "ExposureControlMode" in ods else ""
    ds.BurnedInAnnotation = ods.BurnedInAnnotation if "BurnedInAnnotation" in ods else ""
    ds.DistanceSourceToDetector = ods.DistanceSourceToDetector if "DistanceSourceToDetector" in ods else ""
    ds.DistanceSourceToPatient = ods.DistanceSourceToPatient if "DistanceSourceToPatient" in ods else ""
    ds.PositionerPrimaryAngle = ods.PositionerPrimaryAngle if "PositionerPrimaryAngle" in ods else ""
    ds.PositionerPrimaryAngleDirection = ods.PositionerPrimaryAngleDirection if "PositionerPrimaryAngleDirection" in ods else ""
    ds.PositionerSecondaryAngle = ods.PositionerSecondaryAngle if "PositionerSecondaryAngle" in ods else ""
    ds.ImageLaterality = ods.ImageLaterality if "ImageLaterality" in ods else ""
    ds.BreastImplantPresent = ods.BreastImplantPresent if "BreastImplantPresent" in ods else ""
    ds.Manufacturer = ods.Manufacturer if "Manufacturer" in ods else ""
    ds.ManufacturerModelName = ods.ManufacturerModelName if "ManufacturerModelName" in ods else ""
    ds.EstimatedRadiographicMagnificationFactor = ods.EstimatedRadiographicMagnificationFactor if "EstimatedRadiographicMagnificationFactor" in ods else ""
    ds.DateOfLastDetectorCalibration = ods.DateOfLastDetectorCalibration if "DateOfLastDetectorCalibration" in ods else ""

    filename = clean_string(str(anon_values['studyID'])+"_"+str(ds.SeriesNumber)+"_"+str(ds.InstanceNumber)+"_"+str(ds.Modality)+"_"+str(ds.StudyDescription)+"_"+str(ds.SeriesDescription)+"_"+str(ds.ViewPosition)+".dcm")

    # Create study directory, if it doesn't already exist.
    if grouping == 's':
        if not os.path.exists(os.path.join(out_dir, str(anon_values['studyID']))):
            os.makedirs(os.path.join(out_dir, str(anon_values['studyID'])))
        outpath = os.path.join(out_dir, str(anon_values['studyID']), filename)
    elif grouping == 'm':
        if not os.path.exists(os.path.join(out_dir, str(anon_values['mrn']))):
            os.makedirs(os.path.join(out_dir, str(anon_values['mrn'])))
        outpath = os.path.join(out_dir, str(anon_values['mrn']), filename)
    else:
        outpath = os.path.join(out_dir, filename)
    ds.save_as(outpath, write_like_original=False)


def anonymize_dicoms_mp(link_dict, partition_queue, directory, max_values, directory_num, out_dir, grouping, space, count, start_time):
    if partition_queue[directory]['size'] > space.value:
        print('Ran out of space to write files.')
        logger.warning('Ran out of space to write files.')
        return

    for f in partition_queue[directory]['queue']:
        ds = pydicom.read_file(f)
        keys = ds.dir()
        keys_file_meta = ds.file_meta.dir()

        values = (ds.PatientID, ds.AccessionNumber, ds.StudyInstanceUID, ds.SeriesInstanceUID, ds.SOPInstanceUID)
        anon_values = {}
        for identifier in IDENTIFIER_FIELDS:
            anon_values[identifier] = None

        for i_iter in range(len(DICOM_FIELDS)):
            # Create a unique link between dicom info and anonymous keys to be stored.
            if DICOM_FIELDS[i_iter] in ds:
                if str(values[i_iter]) in link_dict[LINK_LOG_FIELDS[i_iter]]:
                    temp_anon_value = link_dict[LINK_LOG_FIELDS[i_iter]][str(values[i_iter])]
                    anon_values[IDENTIFIER_FIELDS[i_iter]] = temp_anon_value
                else:
                    temp_max_value = max_values[MAX_FIELDS[i_iter]] + 1
                    max_values[MAX_FIELDS[i_iter]] = temp_max_value

                    anon_values[IDENTIFIER_FIELDS[i_iter]] = max_values[MAX_FIELDS[i_iter]]

                    temp_link_dict = link_dict[LINK_LOG_FIELDS[i_iter]]
                    temp_link_dict[str(values[i_iter])] = anon_values[IDENTIFIER_FIELDS[i_iter]]
                    link_dict[LINK_LOG_FIELDS[i_iter]] = temp_link_dict
            else:
                logger.warning("{} not in DICOM store (file: f: ' + f + ')".format(DICOM_FIELDS[i_iter]))

        # If combination of keys already exists in the cache, skip the current dicom.
        dicom_tuple = tuple(anon_values[IDENTIFIER_FIELDS[i_iter]] for i_iter in range(len(IDENTIFIER_FIELDS)))
        if str(dicom_tuple) in link_dict[LINK_LOG_FIELDS[-1]]:
            temp_link_dict_master = link_dict[LINK_LOG_FIELDS[-1]]
            temp_link_dict_master[str(dicom_tuple)] += 1
            link_dict[LINK_LOG_FIELDS[-1]] = temp_link_dict_master

            print('mrn-accession-studyID-seriesID-sopID tuple {} has already been anonymized.'.format(str(dicom_tuple)))
            logger.info('mrn-accession-studyID-seriesID-sopID tuple {} has already been anonymized.'.format(str(dicom_tuple)))
        else:
            temp_link_dict_master = link_dict[LINK_LOG_FIELDS[-1]]
            temp_link_dict_master[str(dicom_tuple)] = 1
            link_dict[LINK_LOG_FIELDS[-1]] = temp_link_dict_master

            try:
                write_dicom(ds, keys, keys_file_meta, anon_values, out_dir, grouping)
            except Exception as error:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logger.warning(
                    "Critical data missing from (file: f: ' + f + ')" + ' error message: ' + str(error) + ' ' + str(
                        exc_type) + ' ' + str(exc_tb.tb_lineno))

    # Account for reduced disk space due to current directory's dicoms.
    space.value -= partition_queue[directory]['size']

    # Remove that directory's dicoms from further consideration.
    del partition_queue[directory]

    count.value += 1
    if not count % 5:
        calculate_progress(count, directory_num, start_time)


def anonymize_dicoms(link_log_path, space, partition_queue, out_dir, grouping, link_dict):
    # Number of directories to analyze
    directory_num = len(partition_queue)
    directories = partition_queue.keys()

    start_time = time.time()

    # Determine where the incrementer stopped in previous runs of the program.
    # Important for creating new identifiers for newly encountered cases.
    max_values = {}
    for i_iter in range(len(MAX_FIELDS)):
        max_values[MAX_FIELDS[i_iter]] = find_max(link_dict[LINK_LOG_FIELDS[i_iter]])

    manager = mp.Manager()
    count = manager.Value('i', 0)
    space = manager.Value('i', space)
    link_dict = manager.dict(link_dict)
    max_values = manager.dict(max_values)
    partition_queue = manager.dict(partition_queue)

    n_cores = mp.cpu_count()
    pool = mp.Pool(n_cores)

    for directory in directories:
        # Check space limitation. Terminate program if space left is too small.
        pool.apply_async(anonymize_dicoms_mp, args=(link_dict, partition_queue, directory, max_values, directory_num, out_dir, grouping, space, count, start_time))
    pool.close()
    pool.join()

    link_dict = link_dict.copy()
    partition_queue = partition_queue.copy()

    # Save cache of already-visited patients.
    for i_iter in range(len(LINK_LOG_FIELDS)):
         save_json(link_dict[LINK_LOG_FIELDS[i_iter]], os.path.join(link_log_path, "{}.json".format(LINK_LOG_FIELDS[i_iter])))

    # Update partition_queue.
    save_json(partition_queue, os.path.join(link_log_path, 'partition_queue.json'))


if __name__ == "__main__":
    # Parse command line arguments.
    args = parse_args()
    dcm_dir = args.dcmdir
    out_dir = args.outdir
    space = float(args.space) * (10**9)
    grouping = args.group

    # Create output directory, if it doesn't already exist.
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Log at WARNING level.
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter(fmt='%(asctime)s:%(levelname)s:%(lineno)d:%(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    handler = logging.FileHandler(os.path.join(args.linklog, 'dcm_anonymize.log'))
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Load partition_queue, if it exists.
    try:
        partition_queue = load_json(os.path.join(args.linklog, 'partition_queue.json'))
    except:
        print('Partition queue does not exist. A new one will be created.')
        logger.info('Partition queue does not exist. A new one will be created.')
        partition_queue = {}

    # Load cache of cases already analyzed, otherwise instantiate new caches
    link_dict = {}
    for i_iter in range(len(LINK_LOG_FIELDS)):
        link_dict[LINK_LOG_FIELDS[i_iter]] = load_link_log(args.linklog, "{}.json".format(LINK_LOG_FIELDS[i_iter]), "{} not found - a new one will be created.".format(LINK_LOG_FIELDS[i_iter]))

    # Load and anonymize dicoms.
    try:
        if not partition_queue:
            partition_queue = get_dicoms(dcm_dir)
            save_json(partition_queue, os.path.join(args.linklog, 'partition_queue.json'))
        anonymize_dicoms(args.linklog, space, partition_queue, out_dir, grouping, link_dict)
    except ValueError:
        print("DICOM file list could not be loaded.")
        logger.error("DICOM file list could not be loaded.")

    print("Anonymization Complete!")
    logger.info("Anonymization Complete!")
