#!/usr/bin/env python3

import argparse
from lxml import etree
from xml.sax.saxutils import unescape
import os


parser = argparse.ArgumentParser(description='Set MaxQuant run un sbatch HPC')
parser.add_argument('template_xml', type=argparse.FileType('r', encoding='UTF-8'))
parser.add_argument('fasta_folder', type=str)
parser.add_argument('raw_file_folders', type=str, nargs='+')
parser.add_argument('-e', '--delete', action='store_true')
parser.add_argument('-t', '--threads', type=int, required=True)
parser.add_argument('-x', '--outxml', type=str, required=True)
parser.add_argument('-o', '--outfolder', type=str, required=True)

args = parser.parse_args()

# Get Fasta file paths
fasta_paths = []
fasta_folder = args.fasta_folder
for f in os.listdir(fasta_folder):
    if os.path.isfile(os.path.join(fasta_folder, f)) and f.lower().endswith(('.fasta', '.fa', '.fas')):
        fasta_paths.append(os.path.join(fasta_folder, f))

# Get RAW file paths
raw_paths = []
for folder in args.raw_file_folders:
    files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    # only take raw files
    files = [f for f in files if f.lower().endswith('.raw')]
    for file in files:
        raw_paths.append(os.path.join(folder, file))

# Prepare output folders
andromeda_idx_path = os.path.join(args.outfolder, 'andromeda')
combined_path = args.outfolder
if not os.path.exists(andromeda_idx_path):
    os.makedirs(andromeda_idx_path)


# Define function to remove file names
def remove_elements(el, subname):
    for subel in el.findall(subname):
        el.remove(subel)


# Define functions to add file names
def add_el_templ(el, templ, field, data_list):
    for d in data_list:
        templ.find(field).text = d
        el.append(templ)


def add_el_name(el, subeltag, data_list):
    for d in data_list:
        a = etree.Element(subeltag)
        a.text = d
        el.append(a)


with open(args.template_xml.name, encoding="utf8") as f:
    tree = etree.parse(f)
root = tree.getroot()

root.find('numThreads').text = str(args.threads)

fasta_files = root.find('fastaFiles')
fasta_templ = fasta_files[0]
fasta_templ.find('fastaFilePath').text = ""
remove_elements(fasta_files, 'FastaFileInfo')
add_el_templ(fasta_files, fasta_templ, 'fastaFilePath', fasta_paths)

raw_files = root.find('filePaths')
remove_elements(raw_files, "string")
add_el_name(raw_files, "string", raw_paths)

if args.delete:
    experiments = root.find('experiments')
    remove_elements(experiments, "string")
    add_el_name(experiments, "string", [""]*len(raw_paths))

    paramGroupIndices = root.find('paramGroupIndices')
    remove_elements(paramGroupIndices, "int")
    add_el_name(paramGroupIndices, "int", ["0"] * len(raw_paths))

fractions = root.find('fractions')
remove_elements(fractions, "short")
add_el_name(fractions, "short", ["32767"]*len(raw_paths))

ptms = root.find('ptms')
remove_elements(ptms, "boolean")
add_el_name(ptms, "boolean", ["False"]*len(raw_paths))

referenceChannel = root.find('referenceChannel')
remove_elements(referenceChannel, "string")
add_el_name(referenceChannel, "string", [""]*len(raw_paths))

remove_elements(root, "fixedSearchFolder")
add_el_name(root, "fixedSearchFolder", [andromeda_idx_path])
remove_elements(root, "fixedCombinedFolder")
add_el_name(root, "fixedCombinedFolder", [combined_path])


etree.indent(root, space="    ")
# etree.indent(root)
text = etree.tostring(root, pretty_print=True, encoding=str, method="xml")
text = unescape(text)
f = open(args.outxml, "w")
f.write(text)
