from __future__ import print_function

import os

# Image handlers
IMPORT_ERROR_MESSAGE = 'could not be imported. This may cause issues, depending on the transfer syntaxes of the dicom files.'
try:
    import numpy
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

from pydicom.pixel_data_handlers.util import pixel_dtype, reshape_pixel_array
try:
    from pydicom.pixel_data_handlers.numpy_handler import get_expected_length
except ImportError:
    from pydicom.pixel_data_handlers.util import get_expected_length

from utils import calculate_age, clean_string


def write_dicom(ods, anon_values, out_dir, grouping):
    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = 'Secondary Capture Image Storage'
    file_meta.MediaStorageSOPInstanceUID = str(anon_values['sopID'])
    file_meta.ImplementationClassUID = '0.0'
    file_meta.TransferSyntaxUID = ods.file_meta.TransferSyntaxUID if "TransferSyntaxUID" in ods.file_meta else "0.0"
    ds = FileDataset(anon_values['studyID'], {}, file_meta=file_meta, preamble="\0"*128)

    ds.Modality = ods.Modality if "Modality" in ods else ""
    ds.StudyDate = "000000"
    ds.StudyTime = "000000"
    ds.StudyInstanceUID = str(anon_values['studyID'])
    ds.SeriesInstanceUID = str(anon_values['seriesID'])
    ds.SOPInstanceUID = str(anon_values['sopID'])
    ds.SOPClassUID = 'Secondary Capture Image Storage'
    ds.SecondaryCaptureDeviceManufctur = 'Python 2.7'

    # These are the necessary imaging components of the FileDataset object.
    ds.AccessionNumber = str(anon_values['accession'])
    ds.PatientID = str(anon_values['mrn'])
    ds.StudyID = str(anon_values['studyID'])

    ds.PatientName = str(anon_values['mrn'])
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
    ds.SeriesNumber = ods.SeriesNumber if "SeriesNumber" in ods else ""
    ds.InstanceNumber = ods.InstanceNumber if "InstanceNumber" in ods else ""
    ds.PlanarConfiguration = ods.PlanarConfiguration if "PlanarConfiguration" in ods else ""
    ds.SamplesPerPixel = ods.SamplesPerPixel if "SamplesPerPixel" in ods else ""
    ds.PhotometricInterpretation = ods.PhotometricInterpretation if "PhotometricInterpretation" in ods else ""
    ds.PixelRepresentation = ods.PixelRepresentation if "PixelRepresentation" in ods else ""
    ds.HighBit = ods.HighBit if "HighBit" in ods else ""
    ds.BitsStored = ods.BitsStored if "BitsStored" in ods else ""
    ds.BitsAllocated = ods.BitsAllocated if "BitsAllocated" in ods else ""
    ds.Columns = ods.Columns if "Columns" in ods else ""
    ds.Rows = ods.Rows if "Rows" in ods else ""
    ds.ImagerPixelSpacing = ods.ImagerPixelSpacing if "ImagerPixelSpacing" in ods else ""

    expected_len = get_expected_length(ods)
    arr = np.frombuffer(ods.PixelData[0:expected_len], dtype=pixel_dtype(ods))
    arr = reshape_pixel_array(ods, arr)
    ds.PixelData = arr.tobytes()

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
    ds.BurnedInAnnotation = ""
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

    filename = clean_string(str(anon_values['mrn']) + "_" + str(anon_values['studyID']) + "_" + str(ds.SeriesNumber) + "_" + str(ds.InstanceNumber) + "_" + str(ds.Modality) + "_" + str(ds.ViewPosition) + ".dcm")

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
