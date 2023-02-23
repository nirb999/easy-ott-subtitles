from typing import List, Dict, Optional

import Utils as Utils
from Singleton import Singleton


####################################################
#
#  EosLanguage
#
####################################################
class EosLanguage:

    __name: str
    __code_639_1: str
    __code_639_2: str
    __code_bcp_47: str

    __model: str
    __enhanced: bool

    __live_delay_seconds: int
    __right_to_left: bool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self,
                 name: str,
                 code_639_1: str,
                 code_639_2: str,
                 code_bcp_47: str,
                 model: str,
                 enhanced: bool,
                 live_delay_seconds: int,
                 right_to_left: bool) -> None:

        self.__name = name
        self.__code_639_1 = code_639_1
        self.__code_639_2 = code_639_2
        self.__code_bcp_47 = code_bcp_47

        self.__model = model
        self.__enhanced = enhanced

        self.__live_delay_seconds = live_delay_seconds
        self.__right_to_left = right_to_left

    ####################################################
    #  __eq__
    ####################################################
    def __eq__(self, other):
        if not isinstance(other, EosLanguage):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.code_bcp_47() == other.code_bcp_47()

    ####################################################
    #  __repr__
    ####################################################
    def __repr__(self) -> str:
        return "EosLanguage: [name:{}, code_639_1:{}, code_639_2:{}, code_bcp_47:{}]".format(self.__name, self.__code_639_1, self.__code_639_2, self.__code_bcp_47)

    ####################################################
    #  name
    ####################################################
    def name(self) -> str:
        return self.__name

    ####################################################
    #  codes
    ####################################################
    def codes(self):
        codes = []
        codes.append(self.__code_639_1)
        codes.append(self.__code_639_2)
        codes.append(self.__code_bcp_47)
        return codes

    ####################################################
    #  code_639_1
    ####################################################
    def code_639_1(self) -> str:
        return self.__code_639_1

    ####################################################
    #  code_639_2
    ####################################################
    def code_639_2(self) -> str:
        return self.__code_639_2

    ####################################################
    #  code_bcp_47
    ####################################################
    def code_bcp_47(self) -> str:
        return self.__code_bcp_47

    ####################################################
    #  model
    ####################################################
    def model(self) -> str:
        return self.__model

    ####################################################
    #  enhanced
    ####################################################
    def enhanced(self) -> bool:
        return self.__enhanced

    ####################################################
    #  live_delay_seconds
    ####################################################
    def live_delay_seconds(self) -> int:
        return self.__live_delay_seconds

    ####################################################
    #  right_to_left
    ####################################################
    def right_to_left(self) -> bool:
        return self.__right_to_left


####################################################
#
#  EosLanguages
#
####################################################
class EosLanguages(metaclass=Singleton):

    __languages: List[EosLanguage]

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__languages = []

        #                                     name               639_1   639_2   bcp_47   model      enhanced   delay   right-to-left
        self.__languages.append(EosLanguage('English (US)',     'en',   'eng',  'en-US', 'video',   True,      60,     False))
        self.__languages.append(EosLanguage('English (UK)',     'en',   'eng',  'en-GB', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Deutsch (DE)',     'de',   'deu',  'de-DE', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Deutsch (CH)',     'de',   'deu',  'de-CH', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Hebrew (IL)',      'he',   'heb',  'iw-IL', 'default', True,      120,     True))
        self.__languages.append(EosLanguage('Spanish (ES)',     'es',   'spa',  'es-ES', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Russian (RU)',     'ru',   'rus',  'ru-RU', 'default', True,      120,    False))
        self.__languages.append(EosLanguage('French (FR)',      'fr',   'fra',  'fr-FR', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Italian (IT)',     'it',   'ita',  'it-IT', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Portuguese (BR)',  'pt',   'por',  'pt-BR', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Arabic (IL)',      'ar',   'ara',  'ar-IL', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Arabic (EG)',      'ar',   'ara',  'ar-EG', 'default', True,      60,     False))
        self.__languages.append(EosLanguage('Arabic (PS)',      'ar',   'ara',  'ar-PS', 'default', True,      60,     False))

    ####################################################
    #  init
    ####################################################
    def init(self) -> None:
        pass

    ####################################################
    #  find
    ####################################################
    def find(self, code: str) -> Optional[EosLanguage]:

        for language in self.__languages:

            if language.code_bcp_47() == code:
                return language

        return None
