import os

# Image handlers
IMPORT_ERROR_MESSAGE = 'could not be imported. This may cause issues, depending on the transfer syntaxes of the dicom files.' \
                       'See https://pydicom.github.io/pydicom/stable/image_data_handlers.html for details.'
try:
    import numpy
    import numpy as np
except ImportError:
    print('Python package numpy', IMPORT_ERROR_MESSAGE)
try:
    import PIL
    from PIL import Image
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

import h5py

from pydicom.dataset import Dataset, FileDataset

import utils


def write_dicom(ods, anon_values, out_dir, grouping):
    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = 'Secondary Capture Image Storage'
    file_meta.MediaStorageSOPInstanceUID = str(anon_values['sopID'])
    file_meta.ImplementationClassUID = '0.0'
    file_meta.TransferSyntaxUID = ods.file_meta.TransferSyntaxUID if "TransferSyntaxUID" in ods.file_meta else "0.0"

    ds = FileDataset(anon_values['studyID'], {}, file_meta=file_meta, preamble=ods.preamble)

    ds.StudyInstanceUID = str(anon_values['studyID'])
    ds.SeriesInstanceUID = str(anon_values['seriesID'])
    ds.SOPInstanceUID = str(anon_values['sopID'])
    ds.SOPClassUID = 'Secondary Capture Image Storage'
    ds.AccessionNumber = str(anon_values['accession'])
    ds.PatientID = str(anon_values['mrn'])
    ds.StudyID = str(anon_values['studyID'])

    ds.PatientName = str(anon_values['mrn'])
    ds.ReferringPhysicianName = ""
    ds.StudyDate = "000000"
    ds.StudyTime = "000000"
    ds.PatientBirthTime = "000000.000000"
    ds.PatientBirthDate = "00000000"
    ds.PatientAge = utils.calculate_age(ods.StudyDate, ods.PatientBirthDate) if ("StudyDate" in ods and "PatientBirthDate" in ods) else ""
    ds.PatientSex = ods.PatientSex if "PatientSex" in ods else ""

    ds.StudyDescription = ods.StudyDescription if "StudyDescription" in ods else ""
    ds.SeriesDescription = ods.SeriesDescription if "SeriesDescription" in ods else ""
    ds.Modality = ods.Modality if "Modality" in ods else ""
    ds.SeriesNumber = ods.SeriesNumber if "SeriesNumber" in ods else ""
    ds.InstanceNumber = ods.InstanceNumber if "InstanceNumber" in ods else ""

    ds.PlanarConfiguration = ods.PlanarConfiguration if "PlanarConfiguration" in ods else ""
    ds.ViewPosition = ods.ViewPosition if "ViewPosition" in ods else ""
    ds.PatientOrientation = ods.PatientOrientation if "PatientOrientation" in ods else ""

    ds.SamplesPerPixel = ods.SamplesPerPixel if "SamplesPerPixel" in ods else ""
    ds.PhotometricInterpretation = ods.PhotometricInterpretation if "PhotometricInterpretation" in ods else ""
    ds.PixelRepresentation = ods.PixelRepresentation if "PixelRepresentation" in ods else ""
    ds.ImagerPixelSpacing = ods.ImagerPixelSpacing if "ImagerPixelSpacing" in ods else ""
    ds.HighBit = ods.HighBit if "HighBit" in ods else ""
    ds.BitsStored = ods.BitsStored if "BitsStored" in ods else ""
    ds.BitsAllocated = ods.BitsAllocated if "BitsAllocated" in ods else ""
    ds.Columns = ods.Columns if "Columns" in ods else ""
    ds.Rows = ods.Rows if "Rows" in ods else ""

    ds.SpecificCharacterSet = ods.SpecificCharacterSet if "SpecificCharacterSet" in ods else ""
    ds.SecondaryCaptureDeviceManufctur = 'Python 3.X'
    ds.PresentationLUTShape = ods.PresentationLUTShape if "PresentationLUTShape" in ods else ""
    ds.KVP = ods.KVP if "KVP" in ods else ""
    ds.XRayTubeCurrent = ods.XRayTubeCurrent if "XRayTubeCurrent" in ods else ""
    ds.ExposureTime = ods.ExposureTime if "ExposureTime" in ods else ""
    ds.Exposure = ods.Exposure if "Exposure" in ods else ""
    ds.ExposureControlMode = ods.ExposureControlMode if "ExposureControlMode" in ods else ""
    ds.RelativeXRayExposure = ods.RelativeXRayExposure if "RelativeXRayExposure" in ods else ""
    ds.FocalSpots = ods.FocalSpots if "FocalSpots" in ods else ""
    ds.AnodeTargetMaterial = ods.AnodeTargetMaterial if "AnodeTargetMaterial" in ods else ""
    ds.BodyPartThickness = ods.BodyPartThickness if "BodyPartThickness" in ods else ""
    ds.CompressionForce = ods.CompressionForce if "CompressionForce" in ods else ""
    ds.PaddleDescription = ods.PaddleDescription if "PaddleDescription" in ods else ""
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

    filename = utils.clean_string('m' + str(anon_values['mrn']) + '_a' + str(anon_values['accession']) + '_st' + str(anon_values['studyID']) + "_se" + str(anon_values['seriesID']) + "_i" + str(anon_values['sopID']) + "_" + str(ds.SeriesNumber) + "_" + str(ds.InstanceNumber) + "_" + str(ds.Modality) + "_" + str(ds.ViewPosition) + ".dcm")

    # Create study directory, if it doesn't already exist.
    if grouping == 'a':
        utils.make_dirs(os.path.join(out_dir, str(anon_values['accession'])))
        out_path = os.path.join(out_dir, str(anon_values['accession']), filename)
    elif grouping == 's':
        utils.make_dirs(os.path.join(out_dir, str(anon_values['studyID'])))
        out_path = os.path.join(out_dir, str(anon_values['studyID']), filename)
    elif grouping == 'm':
        utils.make_dirs(os.path.join(out_dir, str(anon_values['mrn'])))
        out_path = os.path.join(out_dir, str(anon_values['mrn']), filename)
    else:
        out_path = os.path.join(out_dir, filename)

    if 'PixelData' in ods:
        ds.PixelData = ''
        pixel_array = ods.pixel_array
        f = h5py.File('{}.hdf5'.format(out_path[0:-4]))
        f.create_dataset("pixel_array", pixel_array.shape, data=pixel_array, dtype=str(pixel_array.dtype), compression="gzip", shuffle=True)
    ds.save_as(out_path, write_like_original=False)
