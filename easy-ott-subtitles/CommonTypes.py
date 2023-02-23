from typing import Any, Callable, Dict, Optional, Iterable, List
import threading
import math
from enum import Enum
from urllib.parse import urlparse, urlunparse
import base64
# import math

import Utils as Utils
from Singleton import Singleton


####################################################
#
#  EosNames
#
####################################################
class EosNames:

    service_name = 'eos'
    variant_manifest_postfix = 'eos_manifest'
    eos_manifest_prefix = 'eos_manifest'
    fragment_hls_prefix = 'eos_hls_fragment'
    fragment_dash_prefix = 'eos_dash_fragment'
    live_manifest_prefix = 'eos_live'


####################################################
#
#  EosFragmentEncodings
#
####################################################
class EosFragmentEncodings(Enum):
    WAV = 1
    FLAC = 2


####################################################
#
#  EosFragmentEncodings
#
####################################################
class EosHttpConfig:

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'


####################################################
#
#  Context
#
####################################################
class Context:
    """ base class for threads or threadpools which will use FutureResult.
        The function put_job must be implemented in the derived class """

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:
        pass

    ####################################################
    #  put_job
    ####################################################
    def put_job(self, tag: int, class_instance, function: Callable, args: Iterable) -> None:
        Utils.logger_.error('Context', "Context::put_job virtual function called")


####################################################
#
#  Result
#
####################################################
class Result:
    __result: Any
    __is_error: bool
    __error: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, result: Any = None, is_error: bool = False, error: str = '') -> None:

        self.__result = result
        self.__is_error = is_error
        self.__error = error

    ####################################################
    #  result
    ####################################################
    def result(self) -> Any:
        return self.__result

    ####################################################
    #  is_error
    ####################################################
    def is_error(self) -> bool:
        return self.__is_error

    ####################################################
    #  error
    ####################################################
    def error(self) -> str:
        return self.__error


####################################################
#
#  FutureResult
#
####################################################
class FutureResult:
    """ helper class for async operations.
        create FutureResult instance with the details of the context, callback function, class instance, and parameters to the callback fumction.
        start your async operation and pass this FutureResult instance to it. it must be held until the async operation is completed.
        after the callback function is called, this FutureResult will be deleted (no references to it remains). """
    __context: Context
    __tag: int
    __class_instance: Any
    __function: Callable
    __args: Optional[Dict[str, Any]]

    ####################################################
    #  __init__
    ####################################################
    def __init__(self,
                 context: Context,
                 tag: int,
                 class_instance,
                 function: Callable,
                 args: Optional[Dict[str, Any]]) -> None:

        self.__context = context
        self.__tag = tag
        self.__class_instance = class_instance
        self.__function = function
        self.__args = args

    ####################################################
    #  set_result
    ####################################################
    def set_result(self, result: Any = None, is_error: bool = False, error: str = '') -> None:

        _result = Result(result=result, is_error=is_error, error=error)

        if self.__context is None:
            self.__function(self.__class_instance, _result, self.__args)
        else:
            self.__context.put_job(self.__tag, self.__class_instance, self.__function, args=(_result, self.__args))


####################################################
#
#  ExecutionTimerManager
#
####################################################
class ExecutionTimerManager(metaclass=Singleton):
    __lock: threading.Lock
    __timers: Dict[str, Dict[str, Any]]

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        self.__lock = threading.Lock()
        self.__timers = {}

    ####################################################
    #  report_timer
    ####################################################
    def report_timer(self, _name: str, _time: float):

        with self.__lock:
            if _name in self.__timers.keys():
                self.__timers[_name]['sum'] += _time
                self.__timers[_name]['count'] += 1
            else:
                self.__timers[_name] = {}
                self.__timers[_name]['sum'] = _time
                self.__timers[_name]['count'] = 1

    ####################################################
    #  get_averages
    ####################################################
    def get_averages(self) -> Dict[str, float]:

        timers_averages = {}

        with self.__lock:
            for _name in self.__timers.keys():
                timers_averages[_name] = self.__timers[_name]['sum'] / self.__timers[_name]['count']

        return timers_averages


####################################################
#
#  ExecutionTimer
#
####################################################
class ExecutionTimer():

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, name: str):
        self.__name = name
        self.__start = None

    ####################################################
    #  __enter__
    ####################################################
    def __enter__(self):
        import time
        self.__start = time.process_time()

    ####################################################
    #  __exit__
    ####################################################
    def __exit__(self, *args):
        import time

        time_diff = time.process_time() - self.__start

        ExecutionTimerManager().report_timer(self.__name, time_diff)
        # Utils.logger_.debug_color('ExecutionTimer', "timer {}: execution time: {}".format(self.__name, time_diff))


####################################################
#
#  EosUrl
#
####################################################
class EosUrl:

    absolute_url: str
    relative_url: str
    base64_urlsafe: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        self.absolute_url = ''
        self.relative_url = ''
        self.base64_urlsafe = ''

    ####################################################
    #  set_url
    ####################################################
    def set_url(self, url: str, parent_url: str) -> None:

        # print("set_url url={}, parent_url={}".format(url, parent_url))

        parsed_url = urlparse(url)

        if not parsed_url.netloc:
            # url is relative
            self.relative_url = url

            parsed_parent_url = urlparse(parent_url)

            combined_path = parsed_parent_url.path[:parsed_parent_url.path.rfind('/') + 1] + parsed_url.path

            self.absolute_url = urlunparse((parsed_parent_url.scheme,
                                            parsed_parent_url.netloc,
                                            combined_path,
                                            parsed_url.params,
                                            parsed_url.query,
                                            parsed_url.fragment))
        else:
            # url is absolute
            self.absolute_url = url
            self.relative_url = ''

        self.base64_urlsafe = base64.urlsafe_b64encode(str.encode(self.absolute_url)).decode('utf-8')

    ####################################################
    #  __repr__
    ####################################################
    def __repr__(self) -> str:
        return "EosUrl: [absolute_url:{}, relative_url:{}]".format(self.absolute_url, self.relative_url)


####################################################
#
#  EosFragment
#
####################################################
class EosFragment:

    url: EosUrl
    duration: float
    start_time: float
    media_sequence: int  # for HLS
    timestamp: int       # for DASH
    sampling_rate: int
    first_read: bool  # for live

    discontinuity: bool

    encryption_method: str
    encryption_uri: str
    encryption_iv: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        self.url = EosUrl()
        self.duration = 0.0
        self.start_time = 0.0
        self.media_sequence = 0
        self.timestamp = 0
        self.sampling_rate = -1
        self.first_read = False

        self.discontinuity = False

        self.encryption_method = ''
        self.encryption_uri = ''
        self.encryption_iv = ''

    ####################################################
    #  __repr__
    ####################################################
    def __repr__(self) -> str:
        res = "EosFragment: [url:{}, duration:{}, start_time:{}, media_sequence:{}, timestamp:{}, sampling_rate:{}, first_read:{}".format(self.url, self.duration, self.start_time, self.media_sequence, self.timestamp, self.sampling_rate, self.first_read)

        if self.encryption_method != '':
            res += ", encryption_method:{}".format(self.encryption_method)
        if self.encryption_uri != '':
            res += ", encryption_uri:{}".format(self.encryption_uri)
        if self.encryption_iv != '':
            res += ", encryption_iv:{}".format(self.encryption_iv)

        res += "]"

        return res


####################################################
#
#  EosManifest
#
####################################################
class EosManifest:

    url: EosUrl
    manifest_params: Dict[str, str]
    fragments: List[EosFragment]
    eos: bool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        self.url = EosUrl()

        self.manifest_params = {}

        self.fragments = []

        self.eos = False

    ####################################################
    #  __repr__
    ####################################################
    def __repr__(self) -> str:
        return "EosManifest: [url:{}, manifest_params:{}]".format(self.url, self.manifest_params)


####################################################
#
#  LiveDelayListener
#
####################################################
class LiveDelayListener:

    def on_new_fragment(self, fragment: EosFragment, param: str) -> None:
        pass
