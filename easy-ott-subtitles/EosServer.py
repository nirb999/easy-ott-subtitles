import threading

import Utils as Utils
from SessionManager import SessionManager
from HttpMultiServer import HttpMultiServerBaseHandler
from EosRequestResponse import EosSessionRequest, EosSessionResponse


####################################################
#
#  EosHttpHandler
#
####################################################
class EosHttpHandler(HttpMultiServerBaseHandler):

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, request, client_address, server) -> None:

        HttpMultiServerBaseHandler.__init__(self, request, client_address, server)

    ####################################################
    #  __get_id_str
    #  Returns a string representing the module's id
    ####################################################
    def __get_id_str(self) -> str:
        return 'EosHttpHandler'

    ####################################################
    #  _set_headers
    ####################################################
    def _set_headers(self) -> None:
        self.send_response(200)
        # self.send_header('Content-type', 'text/html')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Access-Control-Expose-Headers', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Origin', '*')

    ####################################################
    #  _set_nocache_headers
    ####################################################
    def _set_nocache_headers(self) -> None:
        #self.send_header('Expires', 'Tue, 22 Dec 2020 14:37:34 GMT')
        self.send_header('Cache-Control', 'max-age=0, no-cache, no-store')
        self.send_header('Pragma', 'no-cache')

    ####################################################
    #  _set_cache_headers
    ####################################################
    def _set_cache_headers(self) -> None:
        self.send_header('Cache-Control', 'max-age=604800')

    ####################################################
    #  send_error
    ####################################################
    def send_error(self, msg: str) -> None:
        self.send_response(400)
        self.send_header('Content-type', 'text/html')
        self.send_header('Content-Length', len(str.encode(msg)))
        self.end_headers()
        self.wfile.write(str.encode(msg))

    ####################################################
    #  _handle_get_request
    #  to be implemented in derived class
    ####################################################
    def _handle_get_request(self) -> None:

        self._set_headers()

        request = EosSessionRequest(path=self.path)

        Utils.logger_.info(self.__get_id_str(), "EosHttpHandler::_handle_get_request thread={}, path={}".format(threading.currentThread().getName(), request.parsed_path().path))

        if request.is_valid() is False:
            Utils.logger_.error(self.__get_id_str(), "EosHttpHandler::_handle_get_request request not valid. path: {}".format(self.path))
            self.send_error("Bad Request")
            return

        # get session using rest parameters
        session = SessionManager().get_session(request.rest_key(), 
                                               request.rest_variant_request(),
                                               request.rest_dst_languages(),
                                               request.rest_variants(),
                                               request.rest_delayed_live())

        if session is None:
            response = EosSessionResponse()
            Utils.logger_.error(self.__get_id_str(), "EosHttpHandler::_handle_get_request seesion not found")
            response.error = "session not found"
            return response

        response = session.on_request(request)

        if response.response is not None:
            if response.cache is False:
                self._set_nocache_headers()
            else:
                self._set_cache_headers()
            self.send_header('Content-Type', response.content_type)
            self.send_header('Content-Length', len(response.response))
            self.end_headers()
            self.wfile.write(response.response)
        elif response.error is not None:
            self.send_error(response.error)
        else:
            self.send_error("unknown error")
        return

    ####################################################
    #  _handle_post_request
    #  to be implemented in derived class
    ####################################################
    def _handle_post_request(self) -> None:
        # Doesn't do anything with posted data
        self._set_headers()
        self.end_headers()
        self.wfile.write(str.encode("<html><body><h1>POST!</h1></body></html>"))
