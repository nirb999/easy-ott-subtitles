import threading
from typing import Dict, List, Optional

import Utils as Utils
from Singleton import Singleton
from EosSession import EosSession, EosTranslateSession, EosTranscribeSession
from OttHandler import OttProtocols


####################################################
#
#  SessionManager
#
####################################################
class SessionManager(metaclass=Singleton):
    __sessions: Dict[str, Dict[List[str], EosSession]]  # { key -> { [dst_languages] -> EosSession } }
    __session_ids: Dict[str, EosSession]  # { session_id -> EosSession }
    __lock: threading.Lock

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__sessions = {}
        self.__session_ids = {}

        self.__lock = threading.Lock()

    ####################################################
    #  session_exists
    ####################################################
    def session_exists(self, key: Dict, dst_languages: List[str]) -> bool:

        _key = frozenset(key.items())
        _dst_languages = frozenset(dst_languages)

        if _key in self.__sessions:
            if _dst_languages in self.__sessions[_key]:
                return True

        return False

    ####################################################
    #  get_session
    ####################################################
    def get_session(self, key: Dict, variant_request: bool,
                    dst_languages: List[str], variants: List[int],
                    delayed_live: bool) -> Optional[EosSession]:

        _key = frozenset(key.items())
        _dst_languages = frozenset(dst_languages)

        # if key not found, create new session and return
        if _key not in self.__sessions:

            self.__create_session(key, dst_languages, variants)

            return self.__sessions[_key][_dst_languages]['session']

        # key found, now compare dst_languages
        else:

            # if this is a variant manifest request, dst_languages must match exactly
            if variant_request is True:

                # iterate over all sessions with this key and look for dst_languages
                for dst_lang in self.__sessions[_key]:

                    # if exact match
                    if sorted(dst_languages) == sorted(self.__sessions[_key][dst_lang]['dst_languages']):

                        # return this session
                        return self.__sessions[_key][dst_lang]['session']

            # not variant manifest request (hls fragment manifest or dash/hls fragments or delayed live manifest)
            # requested language must be in dst_languages of the session
            else:

                if delayed_live is True:
                    # return this session
                    return self.__sessions[_key][next(iter(self.__sessions[_key]))]['session']

                # iterate over all sessions with this key and look for dst_languages
                for dst_lang in self.__sessions[_key]:

                    # if contains language
                    if all(elem in self.__sessions[_key][dst_lang]['dst_languages'] for elem in dst_languages):

                        # return this session
                        return self.__sessions[_key][dst_lang]['session']

            # no match, create new session and return
            self.__create_session(key, dst_languages, variants)
            return self.__sessions[_key][_dst_languages]['session']


    ####################################################
    #  remove_session
    ####################################################
    def remove_session(self, session_id: str) -> None:

        if session_id not in self.__session_ids:
            return None

        for key in self.__sessions:
            for dst_lang in self.__sessions[key]:
                if self.__sessions[key][dst_lang]['session'] is self.__session_ids[session_id]:
                    del self.__sessions[key][dst_lang]
                    break

        del self.__session_ids[session_id]

    ####################################################
    #  get_session_by_id
    ####################################################
    def get_session_by_id(self, session_id: str) -> Optional[EosSession]:

        if session_id not in self.__session_ids:
            return None

        return self.__session_ids[session_id]

    ####################################################
    #  __create_session
    ####################################################
    def __create_session(self, key: Dict, dst_languages: List[str], variants: List[int]) -> None:

        Utils.logger_.system('SessionManager', "SessionManager::__create_session key={}, dst_languages={}".format(key, dst_languages))

        if key['protocol'] == 'hls':
            ott_protocol = OttProtocols.HLS_PROTOCOL
        if key['protocol'] == 'dash':
            ott_protocol = OttProtocols.DASH_PROTOCOL

        if key['streaming'] == 'vod':
            live = False
        if key['streaming'] == 'live':
            live = True

        if key['type'] == 'translate':
            new_session = EosTranslateSession(key['origin_url'], ott_protocol, live, dst_languages, key['src_lang'], variants)
        elif key['type'] == 'transcribe':
            new_session = EosTranscribeSession(key['origin_url'], ott_protocol, live, dst_languages, key['src_lang'], variants)

        _key = frozenset(key.items())

        if _key not in self.__sessions:
            self.__sessions[_key] = {}

        _dst_languages = frozenset(dst_languages)

        self.__sessions[_key][_dst_languages] = {'dst_languages': dst_languages, 'session': new_session}
        self.__session_ids[new_session.get_session_id()] = new_session
