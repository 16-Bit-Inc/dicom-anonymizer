import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Anonymizes DICOM directory")

    parser.add_argument("-d",
                        "--dcmdir",
                        type=str,
                        help="Input DICOM directory path",
                        required=True)
    parser.add_argument("-o",
                        "--outdir",
                        type=str,
                        default='./anondata',
                        help="Output DICOM directory path")
    parser.add_argument("-l",
                        "--linklog",
                        type=str,
                        default='./linklog',
                        help="Linking log directory")
    parser.add_argument("-g",
                        "--group",
                        type=str,
                        default='s',
                        help="Group output dicoms into subfolders by anonymized studyID (s) or MRN (m),"
                             "or do not group into subfolders at all (n)")

    args = parser.parse_args()
    return args
