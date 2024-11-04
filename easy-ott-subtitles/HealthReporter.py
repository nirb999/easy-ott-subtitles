from datetime import datetime, timedelta
import threading
import time
from typing import Dict, Any

import Utils as Utils
from Singleton import Singleton
from CommonTypes import ExecutionTimerManager


####################################################
#
#  HealthMonitor
#
####################################################
class HealthMonitor(threading.Thread, metaclass=Singleton):
    __health_check_monitor_: Dict
    __health_check_monitor_lock: threading.Lock

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        # create dict for storing modules responses
        self.__health_check_monitor_ = {}

        # create lock - this class is multithread safe
        self.__health_check_monitor_lock = threading.Lock()

        # init http server thread
        threading.Thread.__init__(self, name='health')

    ####################################################
    #  init
    ####################################################
    def init(self) -> None:
        pass

    ####################################################
    #  __get_id_str
    #  Returns a string representing the module's id
    ####################################################
    def __get_id_str(self) -> str:
        return 'HealthMonitor'

    ####################################################
    #  run
    ####################################################
    def run(self) -> None:
        self.__run_thread()

    ####################################################
    #  __run_thread
    ####################################################
    def __run_thread(self) -> None:

        Utils.logger_.system(self.__get_id_str(), "HealthMonitor::__run_thread thread started name={}".format(self.getName()))

        # run forever
        while True:

            time.sleep(1)

            # self.send_report()

    ####################################################
    #  send_health_beat
    #  this function is called by the modules
    ####################################################
    def send_health_beat(self, module_name: str, status_code: int, status_string: str, additional_data: Dict[str, Any] = {}) -> None:

        # take lock
        self.__health_check_monitor_lock.acquire()

        # update dict
        self.__health_check_monitor_[module_name] = [datetime.utcnow(), status_code, status_string, additional_data]

        # release lock
        self.__health_check_monitor_lock.release()

    ####################################################
    #  get_health_report
    ####################################################
    def get_health_report(self) -> Dict:
        from ThreadPool import JobThreadPool

        json_reply = {}

        json_reply["status"] = "UP"

        json_reply["current_time"] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        json_reply["threadpool_messages"] = JobThreadPool().get_message_count()

        json_reply["threadpool_q_size"] = JobThreadPool().queue_length()

        json_reply["execution_timers"] = ExecutionTimerManager().get_averages()

        json_reply["modules_last_heartbeat"] = {}

        now = datetime.utcnow()

        # take lock
        self.__health_check_monitor_lock.acquire()

        res = 0

        for module_name in self.__health_check_monitor_.keys():

            Utils.logger_.debug(self.__get_id_str(), "HealthMonitor::check_health module {}".format(module_name))

            if now - self.__health_check_monitor_[module_name][0] > timedelta(seconds=10):
                Utils.logger_.error(self.__get_id_str(), "HealthMonitor::check_health module {} late. last beat at {}".format(module_name, self.__health_check_monitor_[module_name][0]))
                res = -1

            if self.__health_check_monitor_[module_name][1] == ModuleBase.STATUS_CODE_ERROR:
                Utils.logger_.error(self.__get_id_str(), "HealthMonitor::check_health module {} status error: {}".format(module_name, self.__health_check_monitor_[module_name][2]))
                res = -1

            json_reply["modules_last_heartbeat"][module_name] = {}
            json_reply["modules_last_heartbeat"][module_name]["last_beat"] = self.__health_check_monitor_[module_name][0].strftime('%Y-%m-%dT%H:%M:%SZ')
            json_reply["modules_last_heartbeat"][module_name]["status_code"] = str(self.__health_check_monitor_[module_name][1])
            json_reply["modules_last_heartbeat"][module_name]["status_text"] = self.__health_check_monitor_[module_name][2]
            json_reply["modules_last_heartbeat"][module_name]["additional_data"] = self.__health_check_monitor_[module_name][3]

        # release lock
        self.__health_check_monitor_lock.release()

        if res == -1:
            json_reply["status"] = "DOWN"

        return json_reply

    ####################################################
    #  send_report
    ####################################################
    def send_report(self) -> None:

        pass

    ####################################################
    #  delivery_report
    ####################################################
    def delivery_report(self, err, msg):
        # Called once for each message produced to indicate delivery result.
        # Triggered by poll() or flush().
        if err is not None:
            Utils.logger_.error(self.__get_id_str(), "HealthMonitor::delivery_report Message delivery failed: {}".format(err))
        else:
            Utils.logger_.debug(self.__get_id_str(), "HealthMonitor::delivery_report Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


####################################################
#
#  ModuleBase
#
#  Each module which is reporting health inherits from this class.
#  It should call 'send_health_check' repeatedly.
#
####################################################
class ModuleBase():
    _module_name: str
    _last_beat: datetime

    STATUS_CODE_OK: int = 0
    STATUS_CODE_ERROR: int = 1

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, module_name: str) -> None:
        self._module_name = module_name

        self._last_beat = datetime.utcnow()

        # call send_health_beat for the first time
        HealthMonitor().send_health_beat(self._module_name, ModuleBase.STATUS_CODE_OK, "UP")

    ####################################################
    #  health_beat_needed
    ####################################################
    def health_beat_needed(self) -> bool:

        now = datetime.utcnow()

        # send only once a second
        if now - self._last_beat > timedelta(seconds=1):
            return True

        return False

    ####################################################
    #  send_health_check
    ####################################################
    def send_health_check(self, status_code: int = STATUS_CODE_OK, status_string: str = "UP", additional_data: Dict[str, Any] = {}) -> None:

        self._last_beat = datetime.utcnow()
        HealthMonitor().send_health_beat(self._module_name, status_code, status_string, additional_data)
