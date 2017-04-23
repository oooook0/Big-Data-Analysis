import time
import psutil
import argparse
import logging
from mpi4py import MPI


def seperate_data(buffer_per_process):

    signal, noise = [], []

    buff_decode = buffer_per_process.decode().split('\n')[:-1]

    lines = [buff_decode[i].split(',') for i in range(0, len(buff_decode))]

    lines.sort()

    for i, line_to_select in enumerate(lines):

        # deal with broken line
        if len(line_to_select) != 3:
            pass
        # remove the invalid data
        elif 'o' in line_to_select[0] or 'O' in line_to_select[0]:
            noise.append(line_to_select)
        # remove the data whose price or volum less or equal than 0
        elif line_to_select[1][0] == '-' or line_to_select[2][0] == '-' or line_to_select[1][0] == '0' or line_to_select[2][0] == '0':
            noise.append(line_to_select)
        elif i > 0 and len(lines[i-1]) == 3:
            # remove the duplicated data
            if line_to_select[0] == lines[i-1][0]:
                noise.append(line_to_select)
            # remove abnormal large or small price
            elif abs(len(line_to_select[1]) - len(lines[i-1][1])) > 1:
                noise.append(line_to_select)
            else:
                signal.append(line_to_select)
        else:
            signal.append(line_to_select)


    signal_data = '\n'.join([','.join(i) for i in signal]) + '\n'
    noise_data = '\n'.join([','.join(i) for i in noise]) + '\n'

    return signal_data,noise_data


if __name__ == "__main__":

    # time tracker
    start = time.time()
    i_time, process_time, on_time, os_time = 0, 0, 0, 0

    # passing the parameters
    parser = argparse.ArgumentParser()
    parser.add_argument("-file", help="Enter the file name", required=True)
    args = parser.parse_args()

    # initial MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # adding log file
    if rank == 0:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename='scrub.log',
                            filemode='w')
        logging.info("initializing MPI...")


    in_file = MPI.File.Open(comm, args.file, MPI.MODE_RDONLY, MPI.INFO_NULL)

    if rank == 0:
        logging.info("open the input file %s" % args.file)

    file_size = in_file.Get_size()
    num_process = comm.Get_size()

    read_size = file_size if file_size < (psutil.virtual_memory().free) / 70 else (psutil.virtual_memory().free) / 70
    size_per_process = int(float(read_size) / num_process)

    iteration = float(file_size) / read_size

    buffer_per_process = bytearray(size_per_process)

    out_file0 = MPI.File.Open(comm, "noise.txt", MPI.MODE_WRONLY | MPI.MODE_CREATE)
    out_file1 = MPI.File.Open(comm, "signal.txt", MPI.MODE_WRONLY | MPI.MODE_CREATE)

    if rank == 0:
        logging.info("file size = %s, read size = %s, size per process = %s" % (file_size, read_size, size_per_process))
        logging.info("iteration = %s" % iteration)

    counter = 0

    while counter < iteration:

        if rank == 0:
            logging.info("-- %s / %i --" % (counter, iteration))

        # read the data
        i_time_start = time.time()

        if counter * read_size + rank * size_per_process + size_per_process <= file_size:
            in_file.Read_at(counter * read_size + rank * size_per_process, buffer_per_process)
        elif counter * read_size + rank * size_per_process > file_size:
            buffer_per_process = bytearray(0)
            in_file.Read_at(counter * read_size + rank * size_per_process, buffer_per_process)
        else:
            buffer_per_process = bytearray(int(file_size - counter * read_size - rank * size_per_process))
            in_file.Read_at(counter * read_size + rank * size_per_process, buffer_per_process)

        i_time_end = time.time()
        i_time += (i_time_end -i_time_start)

        counter += 1

        if rank == 0:
            logging.info("counter %s in iteration %s done on reading" % (counter, iteration))
            logging.info("starting to process the data")

        '''process the data
           1. deal with broken lines
           2. seperate the noise and signal
        '''
        process_time_start = time.time()

        # seperate the data
        signal_data, noise_data = seperate_data(buffer_per_process)

        process_time_end = time.time()
        process_time += (process_time_end - process_time_start)

        # output the data

        if rank == 0:
            logging.info("processing the data is done")
            logging.info("starting to output...")

        # output noise
        on_time_start = time.time()
        out_file0.Write_ordered(noise_data.encode(encoding='UTF-8'))
        on_time_end = time.time()
        on_time += (on_time_end - on_time_start)

        # output signal
        os_time_start = time.time()
        out_file1.Write_ordered(signal_data.encode(encoding='UTF-8'))
        os_time_end = time.time()
        os_time += (os_time_end - os_time_start)


    in_file.Close()
    out_file0.Close()
    out_file1.Close()
    comm.Barrier()
    end = time.time()

    if rank == 0:
        print("Total time of input: {} seconds".format(str(i_time)))
        print("Total time of processing data: {} seconds".format(str(process_time)))
        print("Total time of output signal: {} seconds".format(str(os_time)))
        print("Total time of output noise: {} seconds".format(str(on_time)))
        print("Total time of the whole program: {} seconds".format(str(end - start)))
        logging.info("total input time %s seconds; total processing time %s seconds" % (i_time, process_time))
        logging.info("total output(signal) time %s; total output(noise) time %s" %(os_time, on_time))
        logging.info("total run time %s seconds" % str(end - start))
        logging.info("program ended")