# 16 Bit
# dcmanonymizer.py
# Usage: python dcmanonymizer.py -d <input dicom directory> -o <desired output directory> -l <optional linking log>
# Takes an input directory and creates new anonymized files for each dicom encountered in the directory (recursive for subdirs).
# Also generates a linklog.txt containing "studyID\tAccessionNumber\n" in the same folder as the script if not provided.

import os, sys, getopt, time
import dicom
import cPickle as pickle
import argparse
import dicom, dicom.UID
from dicom.dataset import Dataset, FileDataset
import numpy as np
import datetime, time
from datetime import datetime as dt
from dicom.tag import Tag
from time import gmtime, strftime

def parseArgs():
  parser = argparse.ArgumentParser(description="Anonymizes DICOM directory")
  parser.add_argument("-d", "--dcmdir", type=str, help="Input DICOM directory path", required=True)
  parser.add_argument("-o", "--outdir", type=str, help="Output DICOM directory path", required=True)
  parser.add_argument("-l", "--linklog", type=str, help="Optional linking log path")
  args = parser.parse_args()
  return args

def calculateAge(studyDate, dob):
    d1 = dt.strptime(studyDate, "%Y%m%d")
    d2 = dt.strptime(dob, "%Y%m%d")
    a = abs((d1 - d2).days)/365
    age = str('%03d'%a)+'Y' #format with leading 0
    return age

def cleanString(string):
	charstoremove = ['/', '(', ')', '^', '[', ']', ';', ':']
	for char in charstoremove:
		string = string.replace(char, "")
	
	string = string.replace(" ", "-")
	return string

def getDicoms(dcmdirectory):
   #walks through directory and returns list of all dicom files with absolute paths
   print "Getting dicoms in ", dcmdirectory
   arr = []

   if os.path.isdir(dcmdirectory):
      for root, dirs, files in os.walk(dcmdirectory):
         for name in files:
            if name.endswith((".dcm", ".dicom")):
               arr.append(os.path.join(root,name))
      return arr
   else:
      print "DICOM directory does not exist - check the path"
      return 0   

def writeDicom(ods, mrn, studyID, outdir):

    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = 'Secondary Capture Image Storage'
    file_meta.MediaStorageSOPInstanceUID = '0.0'
    file_meta.ImplementationClassUID = '0.0'
    ds = FileDataset(studyID, {},file_meta = file_meta,preamble="\0"*128)
    ds.Modality = ods.Modality if "Modality" in ods else ""
    ds.StudyDate = ods.StudyDate if "StudyDate" in ods else ""
    ds.StudyTime = ods.StudyTime if "StudyTime" in ods else ""
    ds.StudyInstanceUID =  '0.0'
    ds.SeriesInstanceUID = '0.0'
    ds.SOPInstanceUID =    '0.0'
    ds.SOPClassUID = 'Secondary Capture Image Storage'
    ds.SecondaryCaptureDeviceManufctur = 'Python 2.7.3'

    ## These are the necessary imaging components of the FileDataset object.
    ds.AccessionNumber = str(studyID)
    ds.PatientID = str(mrn)
    ds.StudyID = str(studyID)
    ds.PatientName = str(studyID)
    ds.PatientBirthDate = "00000000"
    ds.PatientAge = calculateAge(ods.StudyDate, ods.PatientBirthDate) if "StudyDate" in ods and "PatientBirthDate" in ods else ""
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
    ds.PixelData = ods.PixelData if "PixelData" in ods else ""
    filename = cleanString(str(studyID)+"_"+str(ds.SeriesNumber)+"_"+str(ds.InstanceNumber)+"_"+str(ds.Modality)+"_"+str(ds.StudyDescription)+"_"+str(ds.SeriesDescription)+"_"+str(ds.ViewPosition)+".dcm")
    outpath = os.path.join(outdir, filename)
    ds.save_as(outpath)
    return

def error_log(msg):
  with open("error_log.txt", "w") as text_file:
    text_file.write(str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))+msg)

def array2txt(arr, filename):
  print "Writing array to " + filename
  with open(filename, 'w') as f: # Write the linkLog
    for s in arr:
    	f.write(str(s[0]) + '\t' + str(s[1]) + '\n')

def txt2array(filename):
   #accepts: filename with text file format <val1>\t<val2>\n
   #returns: nested list (2D array)
   print "Loading ", filename
   arr = []
   try:
      file = open(filename)
   except IOError:
      print "Error: File does not exist"
      return 0

   for line in file.xreadlines():
      line = line.replace('\n', '')
      arr.append(line.split('\t'))

   return arr

def annonymizeDicom(dcmArray, outdir, linkLogProvided):
  fileNum = len(dcmArray)
  count = 0
  startTime = time.time()
  patientList = []
  accessionList = []
  studyID = 0
  patientID = 0

  if not len(linkLogProvided):
  	linkLog = []
  	print "Link log not provided."

  for f in dcmArray:
    ds = dicom.read_file(f)

    if "AccessionNumber" in ds:
      if ds.AccessionNumber not in accessionList:
        accessionList.append(ds.AccessionNumber)
        #if linklog not provided increment studyID and add it to generated linkLog
        #this assumes all files of a study are within one subdirectory (i.e. are processed one study at a time)
        if not len(linkLogProvided):
          studyID += 1
          linkLog.append([studyID, ds.AccessionNumber])

      if len(linkLogProvided):
        #find the appropriate studyID from linkLogProvided
        studyID = 0
        for study in linkLogProvided:
          if study[1] == ds.AccessionNumber:
            studyID = study[0]
            break

        if not studyID:
        	error_log("Accession Number: " + str(ds.AccessionNumber) + " not found in linking log!")
    else:
      error_log("AccessionNumber not in DICOM store (file: " + f + ")")

    if "PatientID" in ds: 
    	# assigns a new mrn to the patient for comparison purposes only
      if ds.PatientID not in patientList:
      	patientList.append(ds.PatientID)
      	patientID += 1
      	mrn = patientID
      else:
      	mrn = patientList.index(ds.PatientID) + 1
    else:
      error_log("MRN not in DICOM store (file: f: " + f + ")")

    writeDicom(ds, mrn, studyID, outdir)
    count += 1
    if not count % 5:
      calculateProgress(count, fileNum, startTime)

  if not len(linkLogProvided):
    array2txt(linkLog, "linklog.txt")

def calculateProgress(count, fileNum, startTime):
   endCycleTime = time.time()
   timeLeft = (endCycleTime - startTime)/count*(fileNum - count)/60.0
   print "---------------" + str(round((count/float(fileNum))*100.0, 1)) + "% Complete (" + str(count) + "/" + str(fileNum) + " | " + str(round(timeLeft, 1)) + " minutes remaining)----------------------"

if __name__ == "__main__":

	args = parseArgs()
	dcmdir = args.dcmdir
	outdir = args.outdir
	linkLog = []

	if not os.path.exists(outdir):
		os.makedirs(outdir)

	if args.linklog is not None and os.path.exists(args.linklog):
		linkLog = txt2array(args.linklog)

	try:
		dicoms = getDicoms(dcmdir)
		annonymizeDicom(dicoms, outdir, linkLog)
	except ValueError:
		print "DICOM filelist could not be loaded"

	print "Anonymization Complete!"