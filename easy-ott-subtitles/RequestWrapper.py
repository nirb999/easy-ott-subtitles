import requests
import datetime
import threading
from typing import Dict, Any

import Utils as Utils
from Singleton import Singleton
from CommonTypes import EosHttpConfig


####################################################
#
#  RequestsStats
#
####################################################
class RequestsStats(metaclass=Singleton):

    __requests: Dict[str, Dict[str, Dict[Any, Any]]]  # { session_id: { request_name: { stats } } }
    __lock: threading.Lock

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__requests = {}

        self.__lock = threading.Lock()

    ####################################################
    #  _check_if_exist
    ####################################################
    def _check_if_exist(self, session_id: str, request_name: str) -> None:

        if session_id not in self.__requests:
            self.__requests[session_id] = {}

        if request_name not in self.__requests[session_id]:
            self.__requests[session_id][request_name] = {}
            self.__requests[session_id][request_name]['success_count'] = 0
            self.__requests[session_id][request_name]['average_time'] = float(0)
            self.__requests[session_id][request_name]['max_time'] = float(0)
            self.__requests[session_id][request_name]['failed_count'] = 0
            self.__requests[session_id][request_name]['failures'] = {}

    ####################################################
    #  add_request_success
    ####################################################
    def add_request_success(self, session_id: str, request_name: str, get_time: datetime.timedelta) -> None:

        self.__lock.acquire()

        self._check_if_exist(session_id, request_name)

        total_time = self.__requests[session_id][request_name]['success_count'] * self.__requests[session_id][request_name]['average_time']
        self.__requests[session_id][request_name]['success_count'] += 1
        self.__requests[session_id][request_name]['average_time'] = (total_time + get_time.total_seconds()) / self.__requests[session_id][request_name]['success_count']

        if get_time.total_seconds() > self.__requests[session_id][request_name]['max_time']:
            self.__requests[session_id][request_name]['max_time'] = get_time.total_seconds()

        self.__lock.release()

    ####################################################
    #  add_request_failed
    ####################################################
    def add_request_failed(self, session_id: str, request_name: str, failure: str) -> None:

        self.__lock.acquire()

        self._check_if_exist(session_id, request_name)

        self.__requests[session_id][request_name]['failed_count'] += 1

        if failure not in self.__requests[session_id][request_name]['failures']:
            self.__requests[session_id][request_name]['failures'][failure] = 0

        self.__requests[session_id][request_name]['failures'][failure] += 1

        self.__lock.release()

    ####################################################
    #  add_request_failed
    ####################################################
    def get_stats(self) -> Dict:

        self.__lock.acquire()

        stats = self.__requests.copy()

        self.__lock.release()

        return stats


####################################################
#
#  RequestWrapper
#
####################################################
class RequestWrapper():
    __session_id: str
    __request_name: str
    __text: bool
    __request_session: requests.Session
    __headers: Dict[str, str]
    __timeout: float
    __max_retries: int

    __use_last_response: bool
    __last_url: str
    __last_response: requests.Response

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, session_id: str, request_name: str) -> None:

        self.__session_id = session_id
        self.__request_name = request_name
        self.__max_retries = 3
        self.__timeout = 3.05

        self.__use_last_response = False
        self.__last_url = ''
        self.__last_response = None

        self.__headers = {'User-Agent': EosHttpConfig.user_agent}

        self.__request_session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(max_retries=self.__max_retries)
        self.__request_session.mount("https://", adapter)
        self.__request_session.mount("http://", adapter)

    ####################################################
    #  use_last_response
    ####################################################
    def use_last_response(self) -> None:
        self.__use_last_response = True

    ####################################################
    #  get
    ####################################################
    def get(self, url: str) -> requests.Response:

        rc = None

        if self.__use_last_response is True and url == self.__last_url and self.__last_response is not None:
            return self.__last_response

        start_time = datetime.datetime.now()

        try:
            response = self.__request_session.get(url, timeout=self.__timeout, headers=self.__headers)

            if response.status_code == requests.codes.ok:
                rc = response
                end_time = datetime.datetime.now()
                get_time = end_time - start_time
                Utils.logger_.dump(self.__session_id, "RequestWrapper::get url={}, time={}".format(url, get_time))
                RequestsStats().add_request_success(self.__session_id, self.__request_name, get_time)
            else:
                RequestsStats().add_request_failed(self.__session_id, self.__request_name, 'StatusCode ' + str(response.status_code))

        except requests.ConnectionError:
            error_str = "HlsLiveDelayHandler::run Error connecting to server {}".format(url)
            Utils.logger_.error(self.__session_id, error_str)
            RequestsStats().add_request_failed(self.__session_id, self.__request_name, 'ConnectionError')
            rc = None
            return rc

        except requests.exceptions.Timeout:
            error_str = "HlsLiveDelayHandler::run Timeout connecting to server {}".format(url)
            Utils.logger_.error(self.__session_id, error_str)
            RequestsStats().add_request_failed(self.__session_id, self.__request_name, 'Timeout')
            rc = None
            return rc

        except requests.exceptions.RequestException:
            error_str = "HlsLiveDelayHandler::run Catastrofic error connecting to server {}".format(url)
            Utils.logger_.error(self.__session_id, error_str)
            RequestsStats().add_request_failed(self.__session_id, self.__request_name, 'RequestException')
            rc = None
            return rc

        if rc is not None:
            self.__last_url = url
            self.__last_response = rc

        return rc
