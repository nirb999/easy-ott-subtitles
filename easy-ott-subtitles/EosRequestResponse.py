from urllib.parse import urlparse, parse_qs
from typing import Optional, List, Dict, Any

import Utils as Utils
from OttHandler import OttProtocols
from Languages import EosLanguages
from CommonTypes import EosNames


# variant hls/dash manifest:
# /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url)}/eos_manifest.[m3u8/mpd]?languages={dst_language}&default={default_lamguage}
#
# hls subtitles manifest:
# /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url}/eos_manifest/{dst_language}/{reference_manifest(base64)}/index.m3u8
#
# hls/dash subtitles fragment:
# /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url}/eos_manifest/{dst_language}/{reference_manifest_url(base64)}/eos_fragment/{reference_fragment_url(base64)}
#
# hls live video/audio/subtitles manifest
# /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url)}/eos_live/{origin_manifest_url}/index.m3u8

# variant hls/dash manifest:
# /eos/{version}/{session_id}/eos_manifest.[m3u8/mpd]?languages={dst_languages}&default={default_lamguage}
#
# hls subtitles manifest:
# /eos/{version}/{session_id}/eos_sub/{dst_language}/{origin_relative_path}
#
# hls/dash subtitles fragment:
# /eos/{version}/{session_id}/eos_sub/{dst_language}/{origin_relative_path}.eos
#
# hls live video/audio/subtitles manifest
# /eos/{version}/{session_id}/eos_live/{origin_manifest_url}/index.m3u8

####################################################
#
#  EosSessionRequest
#
####################################################
class EosSessionRequest:

    __path: str
    __parsed_path: Any
    __parsed_query: Dict[str, Any]
    __tokens: List[str]

    __valid: bool

    __service_name: str
    __version: str
    __version_int: int
    __session_id: str
    __ott_protocol: OttProtocols
    __rest_key: Dict
    __rest_variants: List
    __rest_variant_request: bool
    __rest_delayed_live: bool
    __rest_dst_languages: List

    #################################
    # __init__
    #################################
    def __init__(self, path: str) -> None:

        self.__path = path
        self.__parsed_path = urlparse(self.__path)
        self.__parsed_query = parse_qs(self.__parsed_path.query)

        Utils.logger_.info("EosSessionRequest", "EosSessionRequest::__init__ path={}".format(self.__parsed_path.path))

        self.__tokens = self.__parsed_path.path.split("/")

        i = 0
        for token in self.__tokens:
            Utils.logger_.dump("EosSessionRequest", 'Token {}: {}'.format(i, token))
            i += 1

        self.__valid = False

        self.__service_name = ''
        self.__version = ''
        self.__version_int = 0
        self.__session_id = ''
        self.__ott_protocol = None
        self.__rest_key = {}
        self.__rest_variants = []
        self.__rest_variant_request = False
        self.__rest_delayed_live = False
        self.__rest_dst_languages = []

        if len(self.__tokens) < 5:
            Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ not enough parameters")
            return

        self.__service_name = self.__tokens[1]
        self.__version = self.__tokens[2]

        if self.__service_name != EosNames.service_name:
            Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ service name {} != 'eos'".format(self.__service_name))
            return

        if len(self.__version) < 2 or self.__version[0] != 'v' or self.__version[1:].isdigit() is False:
            Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ bad version format {}".format(self.__version))
            return

        self.__version_int = int(self.__version[1:])

        if self.__version_int != 1:
            # unknow version
            Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ bad version {}".format(self.__version_int))
            return

        if len(self.__tokens) >= 8:

            protocol = self.__tokens[3]
            if protocol != 'hls' and protocol != 'dash':
                Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'protocol' ({})".format(protocol))

            if protocol == 'hls':
                self.__ott_protocol = OttProtocols.HLS_PROTOCOL
            else:
                self.__ott_protocol = OttProtocols.DASH_PROTOCOL

            streaming = self.__tokens[4]
            if streaming != 'vod' and streaming != 'live':
                Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'streaming' ({})".format(streaming))

            eos_type = self.__tokens[5]
            if eos_type != 'translate' and eos_type != 'transcribe' and eos_type != 'ocr':
                Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'eos_type' ({})".format(eos_type))

            src_language = self.__tokens[6]
            if EosLanguages().find(src_language) is None:
                Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'src_language' ({})".format(src_language))

            manifest_url = self.__tokens[7]

            dst_languages = []
            if 'languages' in self.__parsed_query:
                dst_languages = self.__parsed_query['languages'][0].split(',')

            if self.manifest_type() == EosNames.variant_manifest_postfix + '.m3u8' or self.manifest_type() == EosNames.variant_manifest_postfix + '.mpd':
                self.__rest_variant_request = True

                if len(dst_languages) == 0:
                    Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'languages' ({})".format(dst_languages))
                    return
                for dst_lang in dst_languages:
                    if EosLanguages().find(dst_lang) is None:
                        Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'languages' ({})".format(dst_languages))
                        return

                default_lang = self.default_language()
                if default_lang != '':
                    if EosLanguages().find(default_lang) is None:
                        Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'default_language' ({})".format(default_lang))
                        return
            else:
                if self.manifest_type() != EosNames.eos_manifest_prefix and self.manifest_type() != EosNames.live_manifest_prefix:
                    Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong url not found. ({})".format(self.manifest_type()))
                    return

                if self.manifest_type() == EosNames.live_manifest_prefix:
                    self.__rest_delayed_live = True
                    dst_languages = []
                else:
                    if EosLanguages().find(self.dst_lang()) is None:
                        Utils.logger_.error("EosSessionRequest", "EosSessionRequest::__init__ wrong parameter 'language'. path={}".format(self.dst_lang()))
                        return

                    dst_languages = [self.dst_lang()]

        self.__rest_key = {'origin_url': manifest_url,
                           'protocol': protocol,
                           'streaming': streaming,
                           'type': eos_type,
                           'src_lang': src_language}

        self.__rest_variants = []

        self.__rest_dst_languages = dst_languages

        self.__valid = True

    #################################
    # is_valid
    #################################
    def is_valid(self) -> bool:
        return self.__valid

    #################################
    # path
    #################################
    def path(self) -> str:
        return self.__path

    #################################
    # parsed_path
    #################################
    def parsed_path(self) -> str:
        return self.__parsed_path

    #################################
    # num_tokens
    #################################
    def num_tokens(self) -> int:
        return len(self.__tokens)

    #################################
    # service_name
    #################################
    def service_name(self) -> str:
        return self.__service_name

    #################################
    # version
    #################################
    def version(self) -> int:
        return self.__version_int

    #################################
    # session_id
    #################################
    def session_id(self) -> str:
        return self.__session_id

    #################################
    # rest_key
    #################################
    def rest_key(self) -> Dict:
        return self.__rest_key

    #################################
    # rest_variants
    #################################
    def rest_variants(self) -> List:
        return self.__rest_variants

    #################################
    # rest_dst_languages
    #################################
    def rest_dst_languages(self) -> List:
        return self.__rest_dst_languages

    #################################
    # rest_variant_request
    #################################
    def rest_variant_request(self) -> bool:
        return self.__rest_variant_request

    #################################
    # rest_delayed_live
    #################################
    def rest_delayed_live(self) -> bool:
        return self.__rest_delayed_live

    #################################
    # manifest_type
    #################################
    def manifest_type(self) -> str:
        if len(self.__tokens) >= 9:
            return self.__tokens[8]
        return ''

    #################################
    # dst_lang
    # for subtitles fragment/manifest requests
    #################################
    def dst_lang(self) -> str:
        if len(self.__tokens) >= 10:
            return self.__tokens[9]
        return ''

    #################################
    # live_origin_manifest_url
    # for live video/audio/subtitles manifest requests
    #################################
    def live_origin_manifest_url(self) -> str:
        return self.__tokens[9]

    #################################
    # is_variant_manifest_request
    #################################
    def is_variant_manifest_request(self) -> bool:
        return self.__tokens[8].startswith(EosNames.variant_manifest_postfix)

    #################################
    # is_live_manifest_request
    #################################
    def is_live_manifest_request(self) -> bool:
        return self.__tokens[8] == EosNames.live_manifest_prefix

    #################################
    # is_eos_manifest_request
    #################################
    def is_eos_manifest_request(self) -> bool:
        if self.__tokens[8] == EosNames.eos_manifest_prefix or \
           self.__tokens[8] == EosNames.fragment_dash_prefix:
            return True
        return False

    #################################
    # dash_timestamp
    #################################
    def dash_timestamp(self) -> str:
        if len(self.__tokens) >= 13:
            return self.__tokens[12]
        return ''

    #################################
    # is_fragment_request
    #################################
    def is_fragment_request(self) -> bool:
        if len(self.__tokens) < 12:
            return False
        return self.__tokens[11] == EosNames.fragment_hls_prefix

    #################################
    # reference_manifest_url
    #################################
    def reference_manifest_url(self) -> str:
        return self.__tokens[10]

    #################################
    # reference_fragment_url
    #################################
    def reference_fragment_url(self) -> str:
        return self.__tokens[12]

    #################################
    # dash_fragment_url
    #################################
    def dash_fragment_url(self) -> str:
        return self.__tokens[11] + "/" + self.__tokens[12]

    #################################
    # default_language
    #################################
    def default_language(self) -> str:
        default_lang = ''
        if 'default' in self.__parsed_query:
            default_lang = self.__parsed_query['default'][0]
        return default_lang


####################################################
#
#  EosManagementRequest
#
####################################################
class EosManagementRequest:

    __path: str
    __ott_protocol: OttProtocols
    __live: bool

    # variant hls/dash manifest:
    # /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url)}/eos_manifest.[m3u8/mpd]?languages={dst_language}
    #
    # hls subtitles manifest:
    # /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url}/eos_sub/{dst_language}/{origin_relative_path}
    #
    # hls/dash subtitles fragment:
    # /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url}/eos_sub/{dst_language}/{origin_relative_path}.eos
    #
    # hls live video/audio/subtitles manifest
    # /eos/{version}/[hls/dash]/[vod/live]/[translate/transcribe]/{src_language}/{origin_manifest_url(base64url)}/eos_live/{origin_manifest_url}/index.m3u8

    #################################
    # __init__
    #################################
    def __init__(self,
                 path: str,
                 ott_protocol: OttProtocols,
                 live: bool,
                 src_lang: str = '',
                 dst_lang: str = '',
                 reference_manifest_url: str = '',
                 fragment_request: bool = False) -> None:

        self.__path = path
        self.__ott_protocol = ott_protocol
        self.__live = live
        self.__fragment_request = fragment_request
        self.__dst_lang = dst_lang
        self.__reference_manifest_url = reference_manifest_url

    #################################
    # management
    #################################
    def management(self) -> bool:
        return True

    #################################
    # path
    #################################
    def path(self) -> str:
        return self.__path

    #################################
    # is_fragment_request
    #################################
    def is_fragment_request(self) -> bool:
        return False

    #################################
    # dst_lang
    # for subtitles fragment/manifest requests
    #################################
    def dst_lang(self) -> str:
        return self.__dst_lang

    #################################
    # reference_manifest_url
    #################################
    def reference_manifest_url(self) -> str:
        return self.__reference_manifest_url

    #################################
    # default_language
    #################################
    def default_language(self) -> str:
        default_lang = ''
        return default_lang
    
    #################################
    # dash_timestamp
    #################################
    def dash_timestamp(self) -> str:
        return '=Init'


####################################################
#
#  EosSessionResponse
#
####################################################
class EosSessionResponse:

    response: Optional[bytes]
    content_type: str
    cache: bool
    error: Optional[str]

    #################################
    # __init__
    #################################
    def __init__(self) -> None:

        self.response = None
        self.content_type = ''
        self.cache = True
        self.error = None

    #################################
    # is_error
    #################################
    def is_error(self) -> bool:
        if self.error is None:
            return False
        return True
