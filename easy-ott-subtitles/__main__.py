import os
import time
import multiprocessing
import gc
import tracemalloc
import resource
import datetime
import linecache

import Utils as Utils
import Transcoder as Transcoder
from ThreadPool import JobThreadPool
from HealthReporter import HealthMonitor
from Singleton import Singleton
from HttpMultiServer import HttpMultiServer
from EosServer import EosHttpHandler
from Languages import EosLanguages

# config variables
HTTP_SERVER__EOS_HTTP_PORT_NUMBER = Utils.ConfigVariable('HTTP_SERVER', 'EOS_HTTP_PORT_NUMBER', type=int, default_value=8500, description='HTTP server port number', mandatory=False)

APP__NUMBER_OF_THREADS = Utils.ConfigVariable('APP', 'NUMBER_OF_THREADS', type=int, default_value=multiprocessing.cpu_count(), description='Number of worker threads', mandatory=False)


####################################################
#
#  Eos
#
####################################################
class Eos(metaclass=Singleton):

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        # init langiages
        EosLanguages().init()

        # start job threadpool
        JobThreadPool(APP__NUMBER_OF_THREADS.value())

        # start EOS HTTP and HTTPS service
        HttpMultiServer().init(HTTP_SERVER__EOS_HTTP_PORT_NUMBER.value(), EosHttpHandler)
        HttpMultiServer().start()

    ####################################################
    #  __get_id_str
    ####################################################
    def __get_id_str(self) -> str:
        return 'EOS'


####################################################
#
#  display_top
#
####################################################
def display_top(snapshot, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        print("#%s: %s:%s: %.1f KiB"
              % (index, frame.filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))


####################################################
#
#  __main__
#
####################################################
if __name__ == "__main__":

    #tracemalloc.start()
    if tracemalloc.is_tracing() is True:
        print("tracemalloc is tracing")
    traces: int = 0

    if gc.isenabled() is True:
        print("garbage collector is enabled")
    else:
        print("garbage collector is disabled")
    # gc.set_debug(gc.DEBUG_LEAK)

    import argparse
    parser = argparse.ArgumentParser(description="EOS")
    parser.add_argument('-c', '--config-file', help="Path to config file (default='eos.ini')", type=str, default='eos.ini')
    args = parser.parse_args()

    if os.path.isfile(args.config_file) is False:
        print("Error: config file {} not found".format(args.config_file))
        os._exit(0)

    Utils.init_utils(args.config_file, 'eos')
    Transcoder.init_transcoder()

    eos: Eos = Eos()

    # run forever
    prev_snapshot = None
    current_snapshot = None
    while True:

        if tracemalloc.is_tracing() is True:

            if traces == 0:
                traces += 1
                continue
            elif traces == 1:
                prev_snapshot = tracemalloc.take_snapshot()
            else:
                current_snapshot = tracemalloc.take_snapshot()
                display_top(current_snapshot)
            traces += 1

            if traces > 2:
                top_stats = current_snapshot.compare_to(prev_snapshot, 'traceback')
                prev_snapshot = current_snapshot
                print("**********************  tracemalloc  *************************")
                print("{}, trace #{}".format(datetime.datetime.now(), traces))

                trace_size, trace_peak = tracemalloc.get_traced_memory()
                print("current size={}, peak size={}".format(trace_size, trace_peak))

                current_resources = resource.getrusage(resource.RUSAGE_SELF)
                print("maxrss={}, user mode={}, system mode={}".format(current_resources.ru_maxrss, current_resources.ru_utime, current_resources.ru_stime))

                print("[ Top 20 differences ]")
                for stat in top_stats[:20]:
                    print(stat)

                print("**************************************************************\n\n")

        #before_malloc_trim = resource.getrusage(resource.RUSAGE_SELF)
        Utils.malloc_trim()
        #after_malloc_trim = resource.getrusage(resource.RUSAGE_SELF)
        #print("{}, before={}, after={}, user mode={}, system mode={}".format(datetime.datetime.now(), before_malloc_trim.ru_maxrss, after_malloc_trim.ru_maxrss, after_malloc_trim.ru_utime, after_malloc_trim.ru_stime))

        #print("**************************************************************\n\n")

        current = resource.getrusage(resource.RUSAGE_SELF)
        print("{}, max_rss={}, user_mode={}, system_mode={}".format(datetime.datetime.now(), current.ru_maxrss, current.ru_utime, current.ru_stime))

        time.sleep(10)
