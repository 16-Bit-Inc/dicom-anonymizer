# README #

Takes an input directory and creates new deidentified files for each DICOM file encountered in the directory, including subdirectories.

Dependencies: pydicom, python 2.7

Usage: 
```
python dcmanonymizer.py -d <input dicom directory> -o <desired output directory>
```

Rather than delete/replace patient health information in the original DICOM file, this script copies/replaces only the data that would be useful for data analysis and machine learning while still fulfilling requirements of the necessary tags for valid DICOM format. These include:

Modality (copied if present, otherwise blank space)<br />
StudyDate (copied if present, otherwise blank space)<br />
StudyTime (copied if present, otherwise blank space)<br />
StudyInstanceUID ('0.0')<br />
SeriesInstanceUID ('0.0')<br />
SOPInstanceUID ('0.0')<br />
SOPClassUID (copied if present, otherwise blank space)<br />
SecondaryCaptureDeviceManufctur ('Python 2.7.3') <br />
<br />
AccessionNumber (assigned)<br />
PatientID (assigned - patients with same MRN in original DICOMS will have same assigned PatientID)<br />
StudyID (assigned, same as AccessionNumber)<br />
PatientName (replaced with blank space)<br />
PatientBirthDate (replaced with 00000000)<br />
PatientAge (calculated from study date and date of birth)<br />
PatientSex (copied if present, otherwise blank space)<br />
StudyDescription (copied if present, otherwise blank space)<br />
SeriesDescription (copied if present, otherwise blank space)<br />
ViewPosition (copied if present, otherwise blank space)<br />
InstanceNumber (copied if present, otherwise blank space)<br />
SeriesNumber (copied if present, otherwise blank space)<br />
SamplesPerPixel (copied if present, otherwise blank space)<br />
PhotometricInterpretation (copied if present, otherwise blank space)<br />
PixelRepresentation (copied if present, otherwise blank space)<br />
HighBit (copied if present, otherwise blank space)<br />
BitsStored (copied if present, otherwise blank space)<br />
BitsAllocated (copied if present, otherwise blank space)<br />
Columns (copied if present, otherwise blank space)<br />
Rows (copied if present, otherwise blank space)<br />
PixelData (copied if present, otherwise blank space)<br />

The resulting DICOM files are named according to the following convention:
<i>studyID_SeriesNumber_InstanceNumber_Modality_StudyDescription_SeriesDescription_ViewPosition.dcm</i>

Also outputs a linklog.txt containing "studyID\tAccessionNumber\n" in the same folder as the script.

This script is provided "as is" under the MIT license. Please see Licence file for further details.
