import os
import threading
import queue
import time
import traceback
from typing import Dict, List, Optional, Any, Callable, Iterable

import Utils as Utils
from CommonTypes import Context
from Singleton import Singleton
from HealthReporter import ModuleBase

Debug_ThreadPool = False


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


####################################################
#
#  ThreadPoolTags
#
####################################################
ThreadPoolTags = {'free_tag': -1,
                  'classifier_tag': -2}

####################################################
#
#  TagThreadAffinityMap { job_tag: attach_to_thread_id }
#
####################################################
TagThreadAffinityMap = {ThreadPoolTags['classifier_tag']: 1}


####################################################
#
#  ThreadPool
#
####################################################
class ThreadPool(Context):
    """
    Threadpool class which can manage multiple threads.
    It guarentees mutually exclusion for each 'tag', so each tag is handled
    only by one thread at a time. Hence no need for locks in the flow of the jobs.
    There is one 'public' queue for all pending jobs, and a 'private' queue per thread.
    If a thread is dequeing a job from the 'public' queue, and its tag is currently
    being handled by another thread, it will put the job in the other thread's 'private' queue,
    and will deque another job from the 'publiv' queue.
    This way we can ensure maximum efficency of all the threads, while maintaining the order of jobs per tag.
    Make sure no blocking jobs are performed on this context (threadpool).
    For blocking operations you need to switch to another conext/thread, and use the FutureResult class to return
    to this context after blocking job is completed.
    """
    __number_of_threads: int  # number of threads
    __threads: List['ThreadPoolThread']  # data structure to hold the thread instances
    __threads_working_tag: Dict[int, int]  # map of each thread and its current tag in work. [index->tag]
    __queue: queue.Queue  # main queue for incoming jobs
    __private_queues: Dict[int, queue.Queue]  # private queues for each thread
    __lock: threading.Lock
    __message_count: Dict[str, int]  # func_name, count

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, name: str, number_of_threads: int) -> None:

        Context.__init__(self)

        self.__number_of_threads = number_of_threads

        if self.__number_of_threads < 0:
            Utils.logger_.error('ThreadPool', "ThreadPool::__init__ number of threads must be > 0 ({})".format(self.__number_of_threads))
            os._exit(0)

        self.__threads = []

        self.__threads_working_tag = {}  # dict is faster than list in lookup

        self.__queue = queue.Queue()
        self.__private_queues = {}  # dict is faster than list in lookup

        self.__lock = threading.Lock()

        self.__message_count = {}

        start_barrier = threading.Barrier(number_of_threads)  # barrier for starting the threads

        # create the threads
        for i in range(0, self.__number_of_threads):
            thread_name = name + '-' + str(i + 1)
            thread = ThreadPoolThread(thread_name, i + 1, self, start_barrier)
            self.__threads.append(thread)
            self.__threads_working_tag[i + 1] = ThreadPoolTags['free_tag']
            self.__private_queues[i + 1] = queue.Queue()

        # start the threads
        for thread in self.__threads:
            thread.start()

    ####################################################
    #  get_next_job
    ####################################################
    def get_next_job(self, thread_index: int) -> Optional[Dict[str, Any]]:
        """ this function is used by the threads get next job from the threadpool.
            it must not dispatch two jobs with the same tag to more than one thread at any goven time.
            thread_index is the index of the calling thread """

        # first check private queue of this thread. if not empty, return job
        # if self.__private_queues[thread_index].empty() is False:
        try:
            msg = self.__private_queues[thread_index].get(block=False, timeout=0)

            # self.__threads_working_tag[thread_index] = msg['tag']
            if self.__threads_working_tag[thread_index] != msg['tag']:
                print(bcolors.FAIL + "thread {}, __threads_working_tag {}, msg['tag'] {}".format(thread_index, self.__threads_working_tag[thread_index], msg['tag']) + bcolors.ENDC)
            if Debug_ThreadPool is True:
                print(bcolors.OKGREEN + "thread {} starting job tag {} from queue {}".format(thread_index, msg['tag'], 'private') + bcolors.ENDC)
            return msg

        except queue.Empty:
            pass

        msg = None

        # take lock, python is running one thread at a time but the context can switch in the middle of this function
        with self.__lock:

            # release tag
            self.__threads_working_tag[thread_index] = ThreadPoolTags['free_tag']

            # check main queue
            try:
                # queue.get is thread-safe
                msg = self.__queue.get(block=False, timeout=0)
            except queue.Empty as err:
                if Debug_ThreadPool is True:
                    print("thread {} got Empty {}".format(thread_index, err))

            # if no message, return none
            if msg is None:
                return None

            # check if tag affinity is set on this tag
            if msg['tag'] in TagThreadAffinityMap:

                # what is the thread index this tag is attahced to?
                thread_affinty_index = TagThreadAffinityMap[msg['tag']]

                # is this (by chance) the thread we need? if yes, return job
                if thread_index == thread_affinty_index:
                    self.__threads_working_tag[thread_index] = msg['tag']
                    return msg
                else:
                    # put this job in the desired thread's private queue
                    self.__private_queues[thread_affinty_index].put(msg)
                    # return empty msg to thread will retry queue immediatly
                    msg = {'type': 'RETRY', 'tag': None, 'func': None, 'args': None, 'callback': None, 'object': None}
                    return msg

            # check if the tag of the message is in work in another thread
            # if yes, put the message in the private queue of the other thread
            tag_free = True
            for i in range(0, self.__number_of_threads):
                # if Debug_ThreadPool is True: print("thread {} is doing tag {}".format(index, self.__threads_working_tag[i+1][0]))
                if msg['tag'] == self.__threads_working_tag[i + 1]:
                    self.__private_queues[i + 1].put(msg)
                    tag_free = False
                    break

            # tag is free, mark it and return it to requesting thread
            if tag_free is True:
                self.__threads_working_tag[thread_index] = msg['tag']
                if Debug_ThreadPool is True:
                    print(bcolors.OKGREEN + "thread {} starting job tag {} from queue {}".format(thread_index, msg['tag'], 'main') + bcolors.ENDC)
                return msg
            else:
                # return empty msg to thread will retry queue immediatly
                msg = {'type': 'RETRY', 'tag': None, 'func': None, 'args': None, 'callback': None, 'object': None}
                return msg

        return msg

    ####################################################
    #  put_job
    ####################################################
    def put_job(self,
                tag: int,
                class_instance,
                function: Callable,
                args: Iterable) -> None:
        """ use this function to switch contexts, from one thread/threadpool to another.
            tag is the unique identifier of a session/channel/anything tou want to run only in one thread at any time.
            class_instance is the class instance which the function should be called on.
            function is the dunction to be called, usually 'Class.Func' format.
            args are arguments passed to the function. """

        msg = {'type': 'JOB', 'tag': tag, 'class_instance': class_instance, 'function': function, 'args': args}

        with self.__lock:
            if function.__qualname__ in self.__message_count:
                self.__message_count[function.__qualname__] += 1
            else:
                self.__message_count[function.__qualname__] = 1

        if Debug_ThreadPool is True:
            print("put_job type={} tag={} class_instance={} function={} args={}".format(msg['type'],
                                                                                        msg['tag'],
                                                                                        msg['class_instance'],
                                                                                        msg['function'],
                                                                                        msg['args']))

        self.__queue.put(msg)

    ####################################################
    #  queue_length
    ####################################################
    def queue_length(self) -> int:

        return self.__queue.qsize()

    ####################################################
    #  private_queue_length
    ####################################################
    def private_queue_length(self, thread_index: int) -> int:

        return self.__private_queues[thread_index].qsize()

    ####################################################
    #  private_queues_length
    ####################################################
    def private_queues_length(self) -> int:

        sum = 0
        for i in range(0, self.__number_of_threads):
            sum += self.__private_queues[i + 1].qsize()
        return sum

    ####################################################
    #  private_queue_tag
    ####################################################
    def private_queue_tag(self, thread_index: int) -> int:

        return self.__threads_working_tag[thread_index]

    ####################################################
    #  get_message_count
    ####################################################
    def get_message_count(self) -> Dict[str, int]:
        with self.__lock:
            tmp = self.__message_count
        return tmp


####################################################
#
#  ThreadPoolThread
#
####################################################
class ThreadPoolThread(threading.Thread, ModuleBase):
    """ Represents one thread in a threadpool """
    __thread_index: int
    __thread_pool: ThreadPool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, name, index: int, thread_pool: ThreadPool, start_barrier: threading.Barrier):
        ModuleBase.__init__(self, 'thread' + str(index))

        self.__thread_index = index
        self.__thread_pool = thread_pool

        threading.Thread.__init__(self, name=name, target=self.__run_thread, kwargs={'start_barrier': start_barrier})

    ####################################################
    #  __get_id_str
    ####################################################
    def __get_id_str(self):
        return 'ThreadPoolThread_' + str(self.__thread_index)

    ####################################################
    #  __run_thread
    ####################################################
    def __run_thread(self, start_barrier: threading.Barrier):
        """ main loop for a thread """

        if Debug_ThreadPool is True:
            print("ThreadPoolThread::__run_thread thread started name=", self.getName())
        Utils.logger_.system(self.__get_id_str(), "ThreadPoolThread::__run_thread thread started name={}".format(self.getName()))

        # wait for the other threads in the threadpool to become ready
        start_barrier.wait()

        # loop forever
        while True:

            try:

                # get next job from the threadpool
                msg = self.__thread_pool.get_next_job(self.__thread_index)

                if msg is not None:
                    # handle message
                    self.__handle_queue_msg(msg)
                else:
                    # wait 100 msec
                    time.sleep(0.1)

                # send health beat
                if self.health_beat_needed():
                    private_q_size: int = self.__thread_pool.private_queue_length(self.__thread_index)
                    private_q_tag: int = self.__thread_pool.private_queue_tag(self.__thread_index)
                    data = {'private_q_size': private_q_size, 'private_q_tag': private_q_tag}
                    self.send_health_check(additional_data=data)

            except Exception:
                Utils.core_dump_writer_.handle_core("ThreadPoolThread", self.getName(), traceback.format_exc())

    ####################################################
    #  __handle_queue_msg
    ####################################################
    def __handle_queue_msg(self, msg):
        """ handle a job from the queue """

        if msg['type'] == 'JOB':
            # if Debug_ThreadPool is True: print("function= ", msg['function'])
            # if Debug_ThreadPool is True: print("args= ", msg['args'])
            msg['function'](msg['class_instance'], *(msg['args']))
        elif msg['type'] == 'RETRY':
            pass


####################################################
#
#  JobThreadPool
#
####################################################
class JobThreadPool(ThreadPool, metaclass=Singleton):
    """
    Working threads context

    This is the context where all the non-blocking jobs of the
    channels should be executed.
    """

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, number_of_threads: int = 0) -> None:
        ThreadPool.__init__(self, 'job', number_of_threads)
