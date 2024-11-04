from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import Utils as Utils
from Singleton import Singleton


####################################################
#
#  HttpMultiServerBaseHandler
#
####################################################
class HttpMultiServerBaseHandler(BaseHTTPRequestHandler):

    # set to HTTP/1.1 so Expect header will be handeled
    protocol_version = 'HTTP/1.1'

    ####################################################
    #  _get_id_str
    #  Returns a string representing the module's id
    ####################################################
    def _get_id_str(self) -> str:
        return 'HttpMultiServerBaseHandler'

    ####################################################
    #  log_message
    #  override log_message
    ####################################################
    def log_message(self, format, *args):
        Utils.logger_.dump(self._get_id_str(), "%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format%args))

    ####################################################
    #  do_GET
    #  Handle GET requests
    ####################################################
    def do_GET(self) -> None:

        Utils.logger_.system(self._get_id_str(), "HttpMultiServerBaseHandler::do_GET path={}".format(self.path))

        self._handle_get_request()

    ####################################################
    #  do_HEAD
    #  Handle HEAD requests
    ####################################################
    def do_HEAD(self) -> None:
        self.send_response(200)
        self.end_headers()

    ####################################################
    #  do_POST
    #  Handle POST requests
    ####################################################
    def do_POST(self) -> None:

        Utils.logger_.system(self._get_id_str(), "HttpMultiServerBaseHandler::do_POST path={}".format(self.path))

        self._handle_post_request()

    ####################################################
    #  _handle_get_request
    #  to be implemented in derived class
    ####################################################
    def _handle_get_request(self) -> None:
        Utils.logger_.system(self._get_id_str(), "HttpMultiServerBaseHandler::_handle_get_request path={}".format(self.path))
        pass

    ####################################################
    #  _handle_post_request
    #  to be implemented in derived class
    ####################################################
    def _handle_post_request(self) -> None:
        Utils.logger_.system(self._get_id_str(), "HttpMultiServerBaseHandler::_handle_post_request path={}".format(self.path))
        pass


####################################################
#
#  HttpMultiServer
#  using ThreadingHTTPServer for new thread per request
####################################################
class HttpMultiServer(threading.Thread, metaclass=Singleton):
    __http_port: int

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__http_port = 0

        # init http server thread
        threading.Thread.__init__(self, name='http-multi')

    ####################################################
    #  init
    ####################################################
    def init(self, http_port: int, handler_class) -> None:

        self.__http_port = http_port
        self.__handler_class = handler_class

    ####################################################
    #  __get_id_str
    #  Returns a string representing the module's id
    ####################################################
    def __get_id_str(self) -> str:
        return 'HttpMultiServer'

    ####################################################
    #  run
    ####################################################
    def run(self) -> None:
        self.__run_thread()

    ####################################################
    #  __run_thread
    ####################################################
    def __run_thread(self) -> None:

        Utils.logger_.system(self.__get_id_str(), "HttpMultiServer::__run_thread thread started name={}".format(self.getName()))

        # start HTTP server
        server = ThreadingHTTPServer(('', self.__http_port), self.__handler_class)

        # Wait forever for incoming http requests
        server.serve_forever()
