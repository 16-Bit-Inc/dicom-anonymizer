# DICOM ANONYMIZER #

Takes an input directory, and creates new de-identified files for each DICOM file encountered in the directory, including subdirectories.

Please see the pdf file Summary of the Medical Imaging Data Anonymization Program.pdf for a non-technical overview of dcmAnonymizerV02.py's functionality.
For any questions and comments regarding usage and technical details,
please send correspondences to *Daniel Eftekhari* at daniel@16bit.ai

The multiprocessing version of the program, dcmAnonymizerV02MP.py makes use of multiprocessing to speed up the anonymization process.

Software requirements:
1. Anaconda Distribution Python version 3
Download Anaconda at [https://www.anaconda.com/download/](https://www.anaconda.com/download/)
2. After installing Anaconda, add /PATH/TO/ANACONDA and /PATH/TO/ANACONDA/SCRIPTS to your system PATH.
3. Please make sure pydicom has been installed prior to running this program.
This can be done by entering `conda install -c conda-forge pydicom` in the command line/prompt.
4. There are several image handler packages which may or may not be needed depending on your dicom transfer syntaxes.
These packages are imported by default. Only gdcm and jpeg_ls need to be installed manually (assuming Anaconda Distribution is being used).
This can be done by running `conda install -c conda-forge gdcm` to install gdcm, and cloning the CharPyLs repository from https://github.com/Who8MyLunch/CharPyLS and running `pip3 install .` from inside the CharPyLs directory.
See [info on data handlers](https://pydicom.github.io/pydicom/dev/old/image_data_handlers.html) for specifications on which handlers may be needed for your dicom files.

Usage:
```
python3 dcmAnonymizerV02.py -d <input directory> -o <output directory> -l <linking log directory> -g <a/s/m/n>
```

Program input:
1. Top-level directory containing all dicoms, either directly within the directory, or in subdirectories.

Notes:
1. For the same dataset, the path of the linking log folder must be consistent across different runs of the program.
2. If the program terminates because extra disk space is needed to write dicoms to the output folder,
run the program again as many times as needed, each time with a new output folder containing additional disk space.

Program output:
1. For each dicom in the input directory (recursive for subdirectories), if it doesn't already exist, the program writes an anonymized version to the desired output directory.
2. Generates or updates existing link log files (the underlying data structure is a hash table). These are used to determine whether a dicom has already been anonymized or not.


This script is provided "as is" under the MIT license. If you find it useful for your project or publication, please cite it. Please see Licence file for further details.
