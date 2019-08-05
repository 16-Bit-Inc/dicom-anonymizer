from __future__ import print_function

import os

import datetime

import json


def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def load_json(file_name):
    with open(file_name) as data_file:
        data = json.load(data_file)
    return data


def save_json(data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(obj=data, fp=outfile, sort_keys=True, indent=4, separators=(',', ': '))


def load_link_log(logger, link_log_path, file_name, message):
    if (link_log_path is not None) and os.path.exists(os.path.join(link_log_path, file_name)):
        link_dict = load_json(os.path.join(link_log_path, file_name))
        print(message)
        logger.info(message)
    else:
        link_dict = {}
    return link_dict


def calculate_age(study_date, dob):
    if study_date and dob:
        d1 = datetime.datetime.strptime(study_date, "%Y%m%d")
        d2 = datetime.datetime.strptime(dob, "%Y%m%d")
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


def find_max(link_dict):
    """
    Finding the maximum of a hash table's values is an O(N) operation, where N is the number of keys already present
    in the hash table <link_dict>. However, because we are only computing this value once between program runs,
    and in fact not even once for a one-time run, there is no need to optimize efficiency via a (for example) max-heap.
    """
    if link_dict:
        max_value = max(link_dict.values())
    else:
        max_value = 0
    return max_value
