import time
import psutil
import argparse
import logging
# import matplotlib.pyplot as plt
from scipy import stats
from mpi4py import MPI

def get_return_data(buffer_per_process):

    sample_data = []
    dateSet = set()

    buff_decode = buffer_per_process.decode().split('\n')[:-1]
    buff_decode.sort()

    for i, line_to_select in enumerate(buff_decode):
        tmp = line_to_select.split(',')
        # deal with broken line
        if len(tmp) != 3:
            pass
        else:
            # get the first element from the each new second
            if tmp[0][:17] not in dateSet and float(tmp[1]) > 1.:
                dateSet.add(tmp[0][:17])
                sample_data.append(float(tmp[1]))
            else:
                pass
    # get the return
    returns_data = [(float(sample_data[i]) - float(sample_data[i - 1])) / float(sample_data[i - 1]) for i in
                    xrange(1, len(sample_data))]

    return returns_data


if __name__ == "__main__":

    # time tracker
    start = time.time()
    i_time, process_time = 0, 0

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
                            filename='normal.log',
                            filemode='w')
        logging.info("initializing MPI...")


    in_file = MPI.File.Open(comm, args.file, MPI.MODE_RDONLY, MPI.INFO_NULL)

    if rank == 0:
        logging.info("open the input file %s" % args.file)

    file_size = in_file.Get_size()
    num_process = comm.Get_size()

    read_size = file_size if file_size < (psutil.virtual_memory().free) / 80 else (psutil.virtual_memory().free) / 80
    size_per_process = int(float(read_size) / num_process)
    iteration = float(file_size) / read_size
    buffer_per_process = bytearray(size_per_process)

    if rank == 0:
        logging.info("file size = %s, read size = %s, size per process = %s" % (file_size, read_size, size_per_process))
        logging.info("iteration = %s" % iteration)

    counter = 0
    returns_data = []

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
            buffer_per_process = bytearray(file_size - counter * read_size - rank * size_per_process)
            in_file.Read_at(counter * read_size + rank * size_per_process, buffer_per_process)

        i_time_end = time.time()
        i_time += (i_time_end - i_time_start)
        counter += 1

        if rank == 0:
            logging.info("counter %s in iteration %s done on reading" % (counter, iteration))
            logging.info("starting to process the data")

        returns_data += get_return_data(buffer_per_process)

    # gather all the information
    gather_time_start = time.time()
    combine_returns_data = comm.gather(returns_data, root = 0)
    gather_time_end = time.time()

    in_file.Close()

    if rank == 0:
        process_time_start = time.time()
        sample_to_test = []
        for i in combine_returns_data:
            sample_to_test += i
        # get some characteristic of the sample data
        logging.info(stats.describe(sample_to_test))

        # k = z-score returned by skewtest and k is the z-score returned by kurtosistest. p = p-value
        k, p = stats.mstats.normaltest(sample_to_test)
        # we use p value to test if the variable is normal or not
        if p < 0.05:
          print 'the return data is not normal'
          logging.info('normaltest: k = %s, p_value = %s < 0.05 ---> return data is not normal'%(k, p))
        else:
          print 'the return data is normal'
          logging.info('normaltest: k = %s, p_value = %s > 0.05 ---> the return data is normal'%(k, p))


        # plt.subplot(1, 1, 1)
        # plt.hist(sample_to_test)
        # plt.xlabel("the return")
        # plt.savefig("hist_of_return.png")
        # logging.info("output the histgram of the return")

        process_time_end = time.time()
        process_time += (process_time_end - process_time_start)

        end = time.time()

        print "Total time of input: {} seconds".format(str(i_time))
        print "Total time of processing data: {} seconds".format(str(process_time))
        print "Total communication time: {} seconds".format(str(gather_time_end - gather_time_start))
        print "Total time of the whole program: {} seconds".format(str(end - start))
        logging.info("total input time %s seconds; total processing time %s seconds" % (i_time, process_time))
        logging.info("total communication time: %s seconds" % str(gather_time_end - gather_time_start))
        logging.info("total run time %s seconds" % str(end - start))
        logging.info("program ended")