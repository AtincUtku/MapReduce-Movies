import csv

input_file = 'movies.csv'
output_file = 'movies.tsv'

with open(input_file, 'r') as csv_file, open(output_file, 'w', newline='') as tsv_file:
    csv_reader = csv.reader(csv_file)
    tsv_writer = csv.writer(tsv_file, delimiter='\t')

    for row in csv_reader:
        tsv_writer.writerow(row)
