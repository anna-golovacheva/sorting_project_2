import os
import csv
import pandas as pd


def create_folder():
    try:
        os.mkdir('../data/temp')
    except FileExistsError:
        print('folder exists')


def save_data_chunk(data, counter):
    with open(f'../data/temp/{counter}.csv', 'w+') as data_chunk:
        data_chunk.writelines(data)


def file_divider(file_name, batch_size):
    create_folder()
    chunk_counter = 0
    lines_counter = 0
    with open(f'../data/{file_name}') as file:
        header = next(file)
        data_for_save = [header]
        for row in file:
            if lines_counter >= batch_size:
                lines_counter = 0
                chunk_counter += 1
                save_data_chunk(data_for_save, chunk_counter)
                data_for_save = [header]

            data_for_save.append(row)
            lines_counter += 1
        chunk_counter += 1
        if len(data_for_save) > 0:
            save_data_chunk(data_for_save, chunk_counter)

    return chunk_counter


def sort_files(chunk_counter, sort_key):
    sort_key_dict = {
        'date': 0,
        'open': 1,
        'high': 2,
        'low': 3,
        'close': 4,
        'volume': 5,
        'Name': 6
    }
    for i in range(chunk_counter):
        df = pd.read_csv(f'../data/temp/{i + 1}.csv')
        df.sort_values(by=[sort_key], inplace=True)
        df.to_csv(index=False)


def write_all_to_file(file_reader, result_file):
    for row in file_reader:
        result_file.writerow(row)


def concatenate_files(chunks, sort_key):
    for i in range(chunks):

        file = open(f'../data/temp/{i + 1}.csv')

        reader = csv.DictReader(file)

        file_for_result = open(f'../data/result{i + 1}.csv', 'w', newline='')
        fieldnames = list(reader.fieldnames)
        result_writer = csv.DictWriter(file_for_result, fieldnames)
        result_writer.writeheader()

        if i == 0:
            write_all_to_file(reader, result_writer)
        else:
            previous_file = open(f'../data/result{i}.csv')
            previous_reader = csv.DictReader(previous_file)

            row_i = reader.__next__()
            previous_file_row = previous_reader.__next__()

            while True:
                if sort_key in ['open', 'high', 'low', 'close']:
                    if row_i[sort_key] != '':
                        var_1 = float(row_i[sort_key])
                    else:
                        var_1 = 0.0
                    if previous_file_row[sort_key] != '':
                        var_2 = float(previous_file_row[sort_key])
                    else:
                        var_2 = 0.0
                elif sort_key == 'volume':
                    if row_i[sort_key] != '':
                        var_1 = int(row_i[sort_key])
                    else:
                        var_1 = 0
                    if previous_file_row[sort_key] != 0:
                        var_2 = int(previous_file_row[sort_key])
                    else:
                        var_2 = 0
                elif sort_key in ['date', 'Name']:
                    var_1 = row_i[sort_key]
                    var_2 = previous_file_row[sort_key]
                else:
                    raise ValueError('no such sort key')

                if var_1 > var_2:
                    result_writer.writerow(previous_file_row)
                    try:
                        previous_file_row = previous_reader.__next__()
                    except:
                        write_all_to_file(reader, result_writer)
                        break

                else:
                    result_writer.writerow(row_i)
                    try:
                        row_i = reader.__next__()
                    except:
                        write_all_to_file(previous_reader, result_writer)
                        break

            previous_file.close()

        file_for_result.close()
        file.close()
#
# def clear_files(batches_count):
#     rmtree('../data/temp')
#     for i in range(batches_count - 1):
#         os.remove(f'../data/result{i + 1}.csv')
#
#     os.rename(f'../data/result{batches_count}.csv', 'data/result.csv')


def global_sorter():
    chunk_counter = file_divider('all_stocks_5yr.csv', 100000)
    sort_files(chunk_counter, 'high')
    concatenate_files(chunk_counter, 'high')

    # file_sorters(batches_count)
    # concatenate_files(batches_count)
    # # clear_files(batches_count)

global_sorter()