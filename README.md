# README #

Takes an input directory and creates new deidentified files for each DICOM file encountered in the directory, including subdirectories.

Dependencies: pydicom, python 2.7

Usage: 
```
python dcmanonymizer.py -d <input dicom directory> -o <desired output directory>
```

Rather than delete/replace patient health information in the original DICOM file, this script copies/replaces only the data that would be useful for data analysis and machine learning while still fulfilling requirements of the necessary tags for valid DICOM format. These include:
Modality (copied if present, otherwise blank space)
StudyDate (copied if present, otherwise blank space)
StudyTime (copied if present, otherwise blank space)
StudyInstanceUID =  '0.0'
SeriesInstanceUID = '0.0'
SOPInstanceUID =    '0.0'
SOPClassUID (copied if present, otherwise blank space)
SecondaryCaptureDeviceManufctur = 'Python 2.7.3'

AccessionNumber (assigned)
PatientID (assigned - patients with same MRN in original DICOMS will have same assigned PatientID)
StudyID (assigned, same as AccessionNumber)
PatientName (replaced with blank space)
PatientBirthDate (replaced with 00000000)
PatientAge (calculated from study date and date of birth)
PatientSex (copied if present, otherwise blank space)
StudyDescription (copied if present, otherwise blank space)
SeriesDescription (copied if present, otherwise blank space)
ViewPosition (copied if present, otherwise blank space)
InstanceNumber (copied if present, otherwise blank space)
SeriesNumber (copied if present, otherwise blank space)
SamplesPerPixel (copied if present, otherwise blank space)
PhotometricInterpretation (copied if present, otherwise blank space)
PixelRepresentation (copied if present, otherwise blank space)
HighBit (copied if present, otherwise blank space)
BitsStored (copied if present, otherwise blank space)
BitsAllocated (copied if present, otherwise blank space)
Columns (copied if present, otherwise blank space)
Rows (copied if present, otherwise blank space)
PixelData (copied if present, otherwise blank space)

The resulting DICOM files are named according to the following convention:
studyID_SeriesNumber_InstanceNumber_Modality_StudyDescription_SeriesDescription_ViewPosition.dcm

Also outputs a linklog.txt containing "studyID\tAccessionNumber\n" in the same folder as the script.
