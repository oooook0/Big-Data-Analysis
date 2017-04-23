import sys
import time
import pandas as pd
from mpi4py import MPI
from functools import wraps

# calculate the running time
def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        if comm.Get_rank() == 0:
            print ("Total time running %s: %s seconds" %(function.func_name, str(t1 - t0)))
        return result
    return function_timer

# open the file
@ fn_timer
def Read_File(comm,file_name):
    """ Open the file

    """
    try:
        rank = comm.Get_rank()
        in_file = MPI.File.Open(comm, file_name, MPI.MODE_RDONLY, MPI.INFO_NULL)
        file_size = in_file.Get_size()

        num_process = comm.Get_size()
        size_per_process = int(float(file_size) / num_process)
        buffer_per_process = bytearray(size_per_process)
        offset_per_process = rank * size_per_process

        in_file.Read_ordered(buffer_per_process)
        in_file.Close()
        tmp1 = buffer_per_process.decode().split('\r\n')[0:-1]
        tmp2 = [tmp1[i].split(',')[0:] for i in xrange(0, len(tmp1))]
        lines = pd.DataFrame(tmp2[i][0].split(':') + tmp2[i][1:] for i in xrange(0, len(tmp2)))
        lines.columns = ['Date', 'Hour', 'Minute', 'Second', 'Price', 'Volume']
        return lines

    except:
        info = sys.exc_info()
        print info[0], ":", info[1]

@fn_timer
def Seperate_Data(lines):
    # add duplicate
    duplicateIndex = lines.duplicated(['Date', 'Hour', 'Minute', 'Second', 'Price'], keep='last')
    duplicateIndex = [i for i in xrange(len(duplicateIndex)) if duplicateIndex[i] == True]

    # add volume or price less than 0
    lessZeroIndex = list(set(list(lines[lines['Volume'] < '0'].index) + list(lines[lines['Price'] < '0'].index)))

    # remove those date whoes percentage is less than 1%
    dateSet = list(set(lines['Date']))
    dateRemove = list()
    for date in dateSet:
        if len(lines[lines['Date'] == date]) / float(len(lines)) < 0.01:
            dateRemove.append(date)

    # confirm that the date is not the latest date
    for date in dateRemove:
        if date == max(dateSet):
            dateRemove.remove(date)

    dateRemoveIndex = []
    for date in dateRemove:
        dateRemoveIndex = dateRemoveIndex + list(lines[lines['Date'] == date].index)

    # index need to mark
    index_to_remove = duplicateIndex + lessZeroIndex + dateRemoveIndex

    # add 'isNoise' column
    isNoise = ['F'] * len(lines)
    for i in index_to_remove:
        isNoise[i] = 'T'
    isNoise = pd.DataFrame(isNoise)
    isNoise.columns = ['isNoise']

    target = pd.concat([lines, isNoise], axis=1)
    return target

@fn_timer
def Write_File(comm,target):
    signal_data = target[target['isNoise'] == 'F']
    noise_data = target[target['isNoise'] == 'T']

    out_file0 = MPI.File.Open(comm, "noise.txt", MPI.MODE_WRONLY | MPI.MODE_CREATE)
    out_file1 = MPI.File.Open(comm, "signal.txt", MPI.MODE_WRONLY | MPI.MODE_CREATE)

    noise_buf = noise_data.to_string().encode(encoding='UTF-8', errors='strict')
    signal_buf = signal_data.to_string().encode(encoding='UTF-8', errors='strict')

    out_file0.Write_ordered(noise_buf)
    out_file1.Write_ordered(signal_buf)
    out_file0.Close()
    out_file1.Close()


if __name__ == "__main__":

    comm = MPI.COMM_WORLD
    dataBefore = Read_File(comm, 'data-small.txt')
    dataAfter = Seperate_Data(dataBefore)
    Write_File(comm,dataAfter)

