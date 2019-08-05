import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Anonymizes DICOM directory")

    parser.add_argument("-d",
                        "--input_dir",
                        type=str,
                        help="Input DICOM directory path",
                        required=True)
    parser.add_argument("-o",
                        "--output_dir",
                        type=str,
                        default='./anondata',
                        help="Output DICOM directory path")
    parser.add_argument("-l",
                        "--link_log_dir",
                        type=str,
                        default='./linklog',
                        help="Linking log directory")
    parser.add_argument("-g",
                        "--group_by",
                        type=str,
                        default='a',
                        help="Group output dicoms into subfolders by"
                             "anonymized accession number (a), Study Instance UID (s), MRN (m),"
                             "or do not group into subfolders at all (n)")

    args = parser.parse_args()
    return args
