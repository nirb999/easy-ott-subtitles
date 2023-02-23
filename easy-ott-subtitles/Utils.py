import logging
from datetime import datetime, timedelta
import time
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import threading
import configparser
import os
import signal
import math
import re
import ctypes
from typing import Dict, List, Optional, Any, Type

from Singleton import Singleton

global config_
config_ = None

global logger_
logger_ = None

global core_dump_writer_
core_dump_writer_ = None

global app_name_
app_name_ = None

global version_manager_
version_manager_ = None

global startup_time
startup_time = datetime.utcnow()


####################################################
#
#  ConfigParser_
#
####################################################
class ConfigParser_:
    __config_file_name: str
    __lock: threading.RLock
    __parameters: List
    __listeners: Dict[str, List[Dict[str, Any]]]
    __parsed: bool
    __old_config: Optional[configparser.ConfigParser]
    __config: configparser.ConfigParser

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, config_file_name: str) -> None:

        self.__config_file_name = config_file_name

        self.__lock = threading.RLock()

        self.__parameters = []

        self.__listeners = {}

        self.__parsed = False

        self.__old_config = None

        self.__config = configparser.ConfigParser()

        ret = self.__config.read(self.__config_file_name)
        if len(ret) == 0:
            print("ConfigParser_::__init__ Error: config file {} not found".format(self.__config_file_name))
        else:
            self.__parsed = True

    ####################################################
    #  __get_id_str
    ####################################################
    def __get_id_str(self) -> str:
        return 'ConfigParser_'

    ####################################################
    #  register_listener
    ####################################################
    def register_listener(self, section: str, option: str, listener, obj, function) -> None:

        key = section.lower() + ":" + option.lower()

        self.__lock.acquire()

        if key not in self.__listeners:
            self.__listeners[key] = []

        self.__listeners[key].append({'section': section, 'option': option, 'listener': listener, 'object': obj, 'function': function})

        # print(self.__listeners[key])

        self.__lock.release()

    ####################################################
    #  reload
    ####################################################
    def reload(self) -> None:

        self.__lock.acquire()

        self.__parsed = False

        self.__old_config = self.__config

        self.__config = configparser.ConfigParser()

        ret = self.__config.read(self.__config_file_name)
        if len(ret) == 0:
            logger_.error(self.__get_id_str(), 'ConfigParser_::reoload config file {} not found'.format(self.__config_file_name))
            self.__config = self.__old_config
            self.__old_config = None
            self.__parsed = True

            self.__lock.release()
            return
        else:
            self.__parsed = True

        # find updated values
        new_sections = self.__config.sections()
        # old_sections = self.__old_config.sections()

        for new_section in new_sections:
            if self.__old_config.has_section(new_section) is True:

                new_options = self.__config.options(new_section)
                # old_options = self.__old_config.options(new_section)

                for new_option in new_options:

                    if self.__old_config.has_option(new_section, new_option) is True:

                        new_val = self.__config.get(new_section, new_option)
                        old_val = self.__old_config.get(new_section, new_option)

                        if new_val != old_val:
                            logger_.system(self.__get_id_str(), 'ConfigParser_::reoload value of {}:{} changed from {} to {}'.format(new_section, new_option, old_val, new_val))

                            key = new_section.lower() + ":" + new_option.lower()

                            if key in self.__listeners:
                                listeners_list = self.__listeners[key]
                                # print('list: ', listeners_list)
                                for listener in listeners_list:
                                    # print('calling listener: ', listener)
                                    getattr(listener['object'], listener['function'])(listener['listener'], listener['section'], listener['option'], old_val, new_val)

        self.__old_config = None

        self.__lock.release()

    ####################################################
    #  parsed
    ####################################################
    def parsed(self) -> bool:
        return self.__parsed

    ####################################################
    #  file_name
    ####################################################
    def file_name(self) -> str:
        return self.__config_file_name

    ####################################################
    #  get_config
    ####################################################
    def get_config(self, section_name: str, option_name: str, default_value: str, mandatory: bool, use_logger: bool = True) -> str:

        ret = default_value

        self.__lock.acquire()

        if self.__parsed is False:

            if mandatory is True:
                log_str = "ConfigParser_:get_config ERROR {} {} not found, mandatory parameter".format(section_name, option_name)
                if use_logger is True:
                    logger_.error(self.__get_id_str(), log_str)
                # print("\033[91m",log_str,"\033[0m")
                os._exit(0)

            self.__lock.release()
            return ret

        key = section_name.lower() + ":" + option_name.lower()
        print_ = True
        if key in self.__parameters:
            print_ = False
        else:
            self.__parameters.append(key)

        try:
            ret = self.__config.get(section_name, option_name)

            if print_ is True:
                log_str = "ConfigParser_:get_config {} {} value is {}".format(section_name, option_name, ret)

                if use_logger is True:
                    logger_.system(self.__get_id_str(), log_str)
                # print("\033[92m",log_str,"\033[0m")

                if mandatory is False:
                    if default_value.lower() != ret.lower():
                        log_str = "ConfigParser_:get_config {} {} value {} differs from default {}".format(section_name, option_name, ret, default_value)
                        if use_logger is True:
                            logger_.system(self.__get_id_str(), log_str)
                        # print("\033[93m",log_str,"\033[0m")

        except (configparser.NoOptionError, configparser.NoSectionError):
            if mandatory is False:
                if print_ is True:
                    log_str = "ConfigParser_:get_config {} {} not found in config file, using default {}".format(section_name, option_name, default_value)
                    if use_logger is True:
                        logger_.system(self.__get_id_str(), log_str)
                    # print("\033[93m",log_str,"\033[0m")
            else:
                log_str = "ConfigParser_:get_config ERROR {} {} not found, mandatory parameter".format(section_name, option_name)
                if use_logger is True:
                    logger_.error(self.__get_id_str(), log_str)
                # print("\033[91m",log_str,"\033[0m")
                os._exit(0)

            # logging.info('ConfigParser_:get_config value of {} {} not found'.format(section_name, option_name))
            ret = default_value

        self.__lock.release()

        return ret.strip()


####################################################
#
#  ConfigWriter
#
####################################################
class ConfigWriter(metaclass=Singleton):
    __config_use_filename_: Optional[str]
    __config: Dict[str, Dict[str, Dict]]

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__config_use_filename_ = None

        self.__config = {}

    ####################################################
    #  init
    ####################################################
    def init(self, config_file_name: str) -> None:

        self.__config_use_filename_ = os.path.splitext(config_file_name)[0] + '.use'

        self.__write_file()

    ####################################################
    #  update_config
    ####################################################
    def update_config(self,
                      section_name: str,
                      option_name: str,
                      value: Any,
                      default_value: Any,
                      description: str,
                      mandatory: bool,
                      _type: type) -> None:

        update: bool = False

        if section_name not in self.__config:
            self.__config[section_name] = {}

        if option_name not in self.__config[section_name]:
            self.__config[section_name][option_name] = {'value': value, 'description': description, 'type': str(_type.__name__), 'default_value': default_value, 'mandatory': mandatory}
            update = True

        else:
            if self.__config[section_name][option_name]['value'] != value:
                self.__config[section_name][option_name]['value'] = value
                update = True

        if update is True:
            self.__write_file()

    ####################################################
    #  __write_file
    ####################################################
    def __write_file(self) -> None:

        if self.__config_use_filename_ is None:
            return

        with open(self.__config_use_filename_, 'w') as used_config_file:

            for section in self.__config:

                used_config_file.write('[' + section + ']\n')

                for option in self.__config[section]:

                    used_config_file.write(option + ' = {}\n'.format(str(self.__config[section][option]['value'])))
                    used_config_file.write(';    description: {}\n'.format(self.__config[section][option]['description']))
                    used_config_file.write(';    type: {}\n'.format(self.__config[section][option]['type']))
                    used_config_file.write(';    default_value: {}\n'.format(str(self.__config[section][option]['default_value'])))
                    used_config_file.write(';    mandatory: {}\n'.format(str(self.__config[section][option]['mandatory'])))
                    used_config_file.write('\n')

                used_config_file.write('\n')


####################################################
#
#  ConfigVariable
#
####################################################
class ConfigVariable:
    __lock: threading.Lock
    __value: Any
    __default_value: Any
    __type: type
    __section_name: str
    __option_name: str
    __description: str
    __mandatory: bool
    __use_logger: bool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self,
                 section_name: str,
                 option_name: str,
                 default_value: Any,
                 type: Type,
                 description: str,
                 mandatory: bool,
                 use_logger: bool = True) -> None:

        self.__lock = threading.Lock()

        self.__value = default_value
        self.__default_value = default_value
        self.__type = type

        self.__section_name = section_name
        self.__option_name = option_name

        self.__description = description

        self.__mandatory = mandatory

        self.__use_logger = use_logger

        ConfigWriter().update_config(self.__section_name,
                                     self.__option_name,
                                     self.__value,
                                     self.__default_value,
                                     self.__description,
                                     self.__mandatory,
                                     self.__type)

    ####################################################
    #  default_value
    ####################################################
    def default_value(self) -> Any:

        return self.__default_value

    ####################################################
    #  value
    ####################################################
    def value(self) -> Any:
        import importlib

        with self.__lock:

            if self.__type != bool:
                default_value = self.__default_value
            else:
                if self.__default_value is True:
                    default_value = 'True'
                else:
                    default_value = 'False'

            self.__value = config_.get_config(self.__section_name, self.__option_name, str(default_value), self.__mandatory, self.__use_logger)

            try:
                module = importlib.import_module(self.__type.__module__)
                cls = getattr(module, self.__type.__name__)
                if cls == bool:
                    if self.__value == 'True':
                        casted_value = True
                    else:
                        casted_value = False
                else:
                    casted_value = cls(self.__value)

            except (ValueError, AttributeError) as err:
                if self.__use_logger is True:
                    logger_.error('ConfigVariable', "ConfigVariable::value error reading {}:{} : ({})".format(self.__section_name, self.__option_name, err))
                else:
                    print("ConfigVariable::value error reading {}:{} : ({})".format(self.__section_name, self.__option_name, err))
                return self.__default_value

            ConfigWriter().update_config(self.__section_name,
                                         self.__option_name,
                                         self.__value,
                                         self.__default_value,
                                         self.__description,
                                         self.__mandatory,
                                         self.__type)

        return casted_value


# config variables
APP__LOG_LEVEL = ConfigVariable('APP', 'LOG_LEVEL', type=str, default_value='info', description='Log level: dump/debug/info/warning/error/critical', mandatory=False, use_logger=False)
APP__STDOUT_LOG_LEVEL = ConfigVariable('APP', 'STDOUT_LOG_LEVEL', type=str, default_value='warning', description='Log level (stdout): dump/debug/info/warning/error/critical', mandatory=False, use_logger=False)
APP__LOG_IN_JSON_FORMAT = ConfigVariable('APP', 'LOG_IN_JSON_FORMAT', type=bool, default_value=True, description='Use JSON format for logs', mandatory=False, use_logger=False)
APP__LOG_IN_COLORS = ConfigVariable('APP', 'LOG_IN_COLORS', type=bool, default_value=True, description='Use colors for logs', mandatory=False, use_logger=False)
APP__LOG_TO_FILE = ConfigVariable('APP', 'LOG_TO_FILE', type=bool, default_value=False, description='Write logs to file', mandatory=False, use_logger=False)
APP__LOG_TO_STDOUT = ConfigVariable('APP', 'LOG_TO_STDOUT', type=bool, default_value=True, description='Write logs to STDOUT', mandatory=False, use_logger=False)
APP__LOG_FILE_LOCATION = ConfigVariable('APP', 'LOG_FILE_LOCATION', type=str, default_value='', description='Path to log file', mandatory=False, use_logger=False)
APP__LOG_FILE_MAX_SIZE = ConfigVariable('APP', 'LOG_FILE_MAX_SIZE', type=int, default_value=10000000, description='Max log file size in bytes', mandatory=False, use_logger=False)
APP__LOG_FILE_BACKUP_COUNT = ConfigVariable('APP', 'LOG_FILE_BACKUP_COUNT', type=int, default_value=20, description='Number of log files to keep', mandatory=False, use_logger=False)
APP__CORE_FILE_LOCATION = ConfigVariable('APP', 'CORE_FILE_LOCATION', type=str, default_value='core', description='Path to core file', mandatory=False, use_logger=False)
APP__CORE_FILE_MAX_SIZE = ConfigVariable('APP', 'CORE_FILE_MAX_SIZE', type=int, default_value=10000000, description='Max core file size in bytes', mandatory=False, use_logger=False)


####################################################
#
#  Logger_
#
####################################################
class Logger_:
    __log_level: str
    __stdout_log_level: str
    __colors: Dict[str, str]
    __log_to_file: bool
    __log_to_stdout: bool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        self.__log_level = APP__LOG_LEVEL.value()
        self.__stdout_log_level = APP__STDOUT_LOG_LEVEL.value()

        self.__log_level_value = self.__log_level_from_string(self.__log_level)
        self.__stdout_log_level_value = self.__log_level_from_string(self.__stdout_log_level)

        log_format = '%(asctime)s:%(levelname)s:%(message)s'
        if APP__LOG_IN_JSON_FORMAT.value() is True:
            log_format = '{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'

        self.__colors = {'pink': '\033[95m',
                         'blue': '\033[94m',
                         'green': '\033[92m',
                         'yellow': '\033[93m',
                         'red': '\033[91m',
                         'ENDC': '\033[0m',
                         'bold': '\033[1m',
                         'underline': '\033[4m'}
        if APP__LOG_IN_COLORS.value() is False:
            self.__colors = {'pink': '',
                             'blue': '',
                             'green': '',
                             'yellow': '',
                             'red': '',
                             'ENDC': '',
                             'bold': '',
                             'underline': ''}

        log_handlers = []

        self.__log_to_file = APP__LOG_TO_FILE.value()

        if self.__log_to_file is True:
            handler = RotatingFileHandler(APP__LOG_FILE_LOCATION.value(),
                                          maxBytes=APP__LOG_FILE_MAX_SIZE.value(),
                                          backupCount=APP__LOG_FILE_BACKUP_COUNT.value())

            formatter = logging.Formatter(log_format)
            handler.setFormatter(formatter)
            handler.setLevel(self.__log_level_value)
            log_handlers.append(handler)

        self.__log_to_stdout = APP__LOG_TO_STDOUT.value()

        if self.__log_to_stdout is True:
            stdout_handler = StreamHandler()
            stdout_formatter = logging.Formatter(log_format)
            stdout_handler.setFormatter(stdout_formatter)
            stdout_handler.setLevel(self.__stdout_log_level_value)
            log_handlers.append(stdout_handler)

        logging.basicConfig(format=log_format, level=self.__log_level_value, handlers=log_handlers)

        logging.addLevelName(logging.DEBUG - 1, 'DUMP')

        if config_.parsed() is True:
            logging.info('Logger_::loaded config from file {}'.format(config_.file_name()))

        logging.getLogger("urllib3").setLevel(logging.WARNING)

    ####################################################
    #  __log_level_from_string
    ####################################################
    def __log_level_from_string(self, log_level: str):
        log_level_value = logging.INFO
        if log_level == 'critical':
            log_level_value = logging.CRITICAL
        elif log_level == 'error':
            log_level_value = logging.ERROR
        elif log_level == 'warning':
            log_level_value = logging.WARNING
        elif log_level == 'info':
            log_level_value = logging.INFO
        elif log_level == 'debug':
            log_level_value = logging.DEBUG
        elif log_level == 'dump':
            log_level_value = logging.DEBUG - 1

        return log_level_value

    ####################################################
    #  __get_id_str
    ####################################################
    def __get_id_str(self) -> str:
        return 'Logger_'

    ####################################################
    #  config_listener
    ####################################################
    # def config_listener(self, section, option, old_val, new_val):
    #    self.system(self.__get_id_str(), 'Logger_::config_listener section={} option={} old_val={} new_val={}'.format(section, option, old_val, new_val))

    #    if section == 'APP' and option == 'LOG_DUMP':
    #        return

    ####################################################
    #  debug
    ####################################################
    def debug(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        logging.debug(log_str)

    ####################################################
    #  debug_color
    ####################################################
    def debug_color(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['yellow'] + log_str + self.__colors['ENDC']
        logging.debug(log_str)

    ####################################################
    #  dump
    ####################################################
    def dump(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        logging.log(logging.DEBUG - 1, log_str)

    ####################################################
    #  system
    ####################################################
    def system(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['blue'] + log_str + self.__colors['ENDC']
        logging.info(log_str)

    ####################################################
    #  info
    ####################################################
    def info(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['green'] + log_str + self.__colors['ENDC']
        logging.info(log_str)

    ####################################################
    #  info_w
    ####################################################
    def info_w(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        # log_str = self.__colors['green'] + log_str + self.__colors['ENDC']
        logging.info(log_str)

    ####################################################
    #  info_y
    ####################################################
    def info_y(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['yellow'] + log_str + self.__colors['ENDC']
        logging.info(log_str)

    ####################################################
    #  warning
    ####################################################
    def warning(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['pink'] + log_str + self.__colors['ENDC']
        logging.warning(log_str)

    ####################################################
    #  error
    ####################################################
    def error(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['red'] + log_str + self.__colors['ENDC']
        logging.error(log_str)

    ####################################################
    #  critical
    ####################################################
    def critical(self, _id: str, log_str: str) -> None:
        log_str = _id + ' ' + log_str
        log_str = self.__colors['red'] + self.__colors['bold'] + log_str + self.__colors['ENDC']
        logging.critical(log_str)


####################################################
#
#  VersionManager_
#
####################################################
class VersionManager_:
    __major_version: int
    __minor_version: int

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):

        self.__major_version = 1
        self.__minor_version = 0

    ####################################################
    #  get_version_str
    ####################################################
    def get_version_str(self) -> str:

        version_str = str(self.__major_version) + '.' + str(self.__minor_version)

        return version_str


####################################################
#
#  CoreDumpWriter
#
####################################################
class CoreDumpWriter:
    __core_file_name: str
    __core_file_max_size: int
    __lock: threading.Lock

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, core_file_name: str, core_file_max_size: int) -> None:

        self.__core_file_name = core_file_name
        self.__core_file_max_size = core_file_max_size

        self.__lock = threading.Lock()

    ####################################################
    #  handle_core
    ####################################################
    def handle_core(self, module_name: str, thread_name: str, traceback_str: str) -> None:

        # first print to log
        logger_.critical(module_name, "Unhandled exception (on thread {}): {}".format(thread_name, traceback_str))

        # check if core dump is enabled
        if self.__core_file_name is None or self.__core_file_name == '' or self.__core_file_max_size == 0:
            return

        with self.__lock:

            # check file size
            if os.path.isfile(self.__core_file_name) is True:
                try:
                    file_size = os.path.getsize(self.__core_file_name)
                except OSError as err:
                    logger_.error("CoreDumpWriter", "CoreDumpWriter::handle_core {}".format(err))
                    return

                if file_size > self.__core_file_max_size:
                    logger_.error("CoreDumpWriter", "CoreDumpWriter::handle_core core file size {} reached limit {}".format(file_size, self.__core_file_max_size))
                    return

            # write to file
            try:
                with open(self.__core_file_name, 'a') as core_file:
                    core_file.write('===========================================================================================')
                    core_file.write('\ndate (utc): {}'.format(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')))
                    core_file.write('\nthread: {}'.format(thread_name))
                    core_file.write('\ntraceback: {}'.format(traceback_str))
                    core_file.write('\n\n')
            except OSError as err:
                logger_.error("CoreDumpWriter", "CoreDumpWriter::handle_core error writing core file: {}".format(err))


####################################################
#
#  PTS
#
####################################################
class PTS:

    __max_val: int = int(math.pow(2, 33))

    __wrap_margin: int = 90000 * 30  # 30 seconds
    __wrap_margin_low: int = __wrap_margin
    __wrap_margin_high: int = __max_val - __wrap_margin

    __pts_val: int
    __orig_pts_val: int

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, pts_val: int) -> None:

        self.__pts_val = pts_val
        self.__orig_pts_val = pts_val

        # fix overflowed values
        while self.__pts_val >= PTS.__max_val:

            if self.__pts_val == self.__orig_pts_val:  # print only once
                logger_.warning('PTS', 'PTS::__init__ value overflow {}'.format(self.__pts_val))

            self.__pts_val = self.__pts_val - PTS.__max_val

    ####################################################
    #  val
    ####################################################
    def val(self) -> int:
        return self.__pts_val

    ####################################################
    #  original_val
    ####################################################
    def original_val(self) -> int:
        return self.__orig_pts_val

    ####################################################
    #  __repr__
    ####################################################
    def __repr__(self) -> str:

        if self.__pts_val == self.__orig_pts_val:
            return str(int(self.__pts_val))

        return str(int(self.__pts_val)) + ' (orig:' + str(int(self.__orig_pts_val)) + ')'

    ####################################################
    #  __add__
    ####################################################
    def __add__(self, other):

        res = self.__pts_val + other.val()

        if res >= PTS.__max_val:
            res = res - PTS.__max_val

        return PTS(res)

    ####################################################
    #  __sub__
    ####################################################
    def __sub__(self, other):

        if self.__pts_val >= other.val():
            return PTS(self.__pts_val - other.val())

        return PTS(self.__pts_val + PTS.__max_val - other.val())

    ####################################################
    #  __eq__
    ####################################################
    def __eq__(self, other):

        if self.__pts_val == other.val():
            return True

        return False

    ####################################################
    #  __ne__
    ####################################################
    def __ne__(self, other):

        if self.__pts_val == other.val():
            return False

        return True

    ####################################################
    #  __hash__
    ####################################################
    def __hash__(self):
        return hash(self.__pts_val)

    ####################################################
    #  __lt__
    ####################################################
    def __lt__(self, other):

        return not self.__gt__(other)

    ####################################################
    #  __le__
    ####################################################
    def __le__(self, other):

        if self.__pts_val == other.val():
            return True

        return self.__lt__(other)

    ####################################################
    #  __gt__
    ####################################################
    def __gt__(self, other):

        if self.__pts_val < PTS.__wrap_margin_low and other.val() > PTS.__wrap_margin_high:
            return True

        if self.__pts_val > PTS.__wrap_margin_high and other.val() < PTS.__wrap_margin_low:
            return False

        if self.__pts_val > other.val():
            return True

        return False

    ####################################################
    #  __ge__
    ####################################################
    def __ge__(self, other):

        if self.__pts_val == other.val():
            return True

        return self.__gt__(other)


####################################################
#
#  init
#
####################################################

####################################################
#  malloc_trim
####################################################
def malloc_trim():
    ctypes.cdll.LoadLibrary('libc.so.6')
    ctypes.CDLL('libc.so.6').malloc_trim(0)


####################################################
#  sighup_handler
####################################################
def sighup_handler(signum, frame):
    logger_.system('sighup_handler', "reloading configuration")

    config_.reload()


####################################################
#  channel_id_to_log_id
####################################################
def channel_id_to_log_id(channel_id: int) -> str:
    return 'channel_' + str(channel_id)


####################################################
#  epoch_to_datetime_str
####################################################
def epoch_to_datetime_str(timestamp):
    timestamp = timestamp / 1000
    datetime_ = datetime.fromtimestamp(timestamp)
    str = datetime_.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return str[:-3] + 'Z'  # only milisec


####################################################
#  datetime_to_str_msec
####################################################
def datetime_to_str_msec(datetime_: datetime):
    str_ = datetime_.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return str_[:-3] + 'Z'  # only milisec


####################################################
#  datetime_to_str
####################################################
def datetime_to_str(datetime_: datetime):
    str_ = datetime_.strftime('%Y-%m-%dT%H:%M:%SZ')
    return str_


####################################################
#  str_to_datetime
####################################################
def str_to_datetime(str_: str) -> datetime:
    timestamp: datetime = datetime.strptime(str_, "%Y-%m-%dT%H:%M:%SZ")
    return timestamp


####################################################
#  get_uptime
####################################################
def get_uptime() -> timedelta:

    return datetime.utcnow() - startup_time


####################################################
#  now_to_epoch
####################################################
def now_to_epoch() -> int:
    return int(round(time.mktime(time.gmtime()) * 1000))


#################################
# seconds_to_srt_time
#################################
def seconds_to_srt_time(time: float) -> str:

    time_millisec_, seconds_ = math.modf(time)

    time_millisec = round(time_millisec_ * 1000)
    seconds = int(seconds_)

    time_hours = int(seconds / 3600)
    time_minutes = int((seconds - time_hours * 3600) / 60)
    time_seconds = (seconds - time_hours * 3600 - time_minutes * 60)

    hours_str = str(time_hours)
    if time_hours < 10:
        hours_str = '0' + str(time_hours)

    minutes_str = str(time_minutes)
    if time_minutes < 10:
        minutes_str = '0' + str(time_minutes)

    seconds_str = str(time_seconds)
    if time_seconds < 10:
        seconds_str = '0' + str(time_seconds)

    millisec_str = str(time_millisec)
    if time_millisec < 10:
        millisec_str = '00' + str(time_millisec)
    elif time_millisec < 100:
        millisec_str = '0' + str(time_millisec)

    srt_time = hours_str + ':' + minutes_str + ':' + seconds_str + ',' + millisec_str

    return srt_time


#################################
# parse_hls_params
#################################
def parse_hls_params(line: str) -> Dict[str, str]:

    PATTERN = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')

    param_dict = {}

    # param_list = line.split(',')
    param_list = PATTERN.split(line)[1::2]

    for param in param_list:
        # print("param: ", param)
        key_val = param.split('=', 1)

        param_dict[key_val[0]] = key_val[1]

    return param_dict


#################################
# seconds_to_webvtt_time
#################################
def seconds_to_webvtt_time(time: float) -> str:

    time_millisec_, seconds_ = math.modf(time)

    time_millisec = round(time_millisec_ * 1000)
    seconds = int(seconds_)

    time_hours = int(seconds / 3600)
    time_minutes = int((seconds - time_hours * 3600) / 60)
    time_seconds = (seconds - time_hours * 3600 - time_minutes * 60)

    hours_str = str(time_hours)
    if time_hours < 10:
        hours_str = '0' + str(time_hours)

    minutes_str = str(time_minutes)
    if time_minutes < 10:
        minutes_str = '0' + str(time_minutes)

    seconds_str = str(time_seconds)
    if time_seconds < 10:
        seconds_str = '0' + str(time_seconds)

    millisec_str = str(time_millisec)
    if time_millisec < 10:
        millisec_str = '00' + str(time_millisec)
    elif time_millisec < 100:
        millisec_str = '0' + str(time_millisec)

    webvtt_time = hours_str + ':' + minutes_str + ':' + seconds_str + '.' + millisec_str

    return webvtt_time


####################################################
#  init_utils
####################################################
def init_utils(config_file: str, application_name: str) -> None:

    ConfigWriter().init(config_file)

    global config_
    config_ = ConfigParser_(config_file)

    global version_manager_
    version_manager_ = VersionManager_()

    global logger_
    logger_ = Logger_()

    global core_dump_writer_
    core_dump_writer_ = CoreDumpWriter(APP__CORE_FILE_LOCATION.value() + "." + str(os.getpid()) + ".dump", APP__CORE_FILE_MAX_SIZE.value())

    global app_name_
    app_name_ = application_name

    logger_.system('init_utils', "Starting {} : verion {} (pid {})".format(application_name, version_manager_.get_version_str(), os.getpid()))

    # Set the signal handler for SIGHUP
    signal.signal(signal.SIGHUP, sighup_handler)
