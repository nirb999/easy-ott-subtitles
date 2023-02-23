import requests
from io import BufferedReader, BytesIO
from collections import deque
from typing import Optional, List, Dict, Any
import binascii
import ctypes

from Crypto.Cipher import AES
from Crypto.Util import Counter
from construct import Container
from pymp4.parser import Box
from pymp4.util import BoxUtil

import Utils as Utils


####################################################
#
#  DashFragmentEncoder
#
####################################################
class DashFragmentEncoder:

    ####################################################
    #  __init__
    ####################################################
    def __init__(self):
        pass

    ####################################################
    #  build_subtitles_initialization
    ####################################################
    def build_subtitles_fragment(self, timescale_val: int, start_time: int, end_time: int, subtitle_ttml: str) -> bytes:

        subtitle_ttml_encoded = subtitle_ttml.encode('utf-8')

        moof = \
            Container(type=b"moof")(children=[
                Container(type=b"mfhd")(sequence_number=1),
                Container(type=b"traf")(children=[
                    Container(type=b"tfhd")(version=0)
                    (flags=Container(default_base_is_moof=True)(duration_is_empty=False)
                    (default_sample_flags_present=False)(default_sample_size_present=True)
                    (default_sample_duration_present=True)(sample_description_index_present=True)(base_data_offset_present=False))
                    (track_ID=1)(sample_description_index=1)(default_sample_size=len(subtitle_ttml_encoded))
                    (default_sample_duration=4000)(base_data_offset=0)(default_sample_flags=None),
                    Container(type=b"tfdt")(version=1)(baseMediaDecodeTime=start_time),
                    Container(type=b"trun")(version=1)(flags=Container(sample_composition_time_offsets_present=True)
                    (sample_flags_present=True)(sample_size_present=True)(sample_duration_present=True)
                    (first_sample_flags_present=False)(data_offset_present=True))
                    (sample_count=1)(data_offset=124)(first_sample_flags=None)
                    (sample_info=[Container(sample_duration=4000)(sample_size=len(subtitle_ttml_encoded))
                    (sample_flags=Container(is_leading=0)(sample_depends_on=0)(sample_is_depended_on=0)
                    (sample_has_redundancy=0)(sample_padding_value=0)(sample_is_non_sync_sample=False)(sample_degradation_priority=0))
                    (sample_composition_time_offsets=0)])
                ])
            ])

        mdat = Container(type=b"mdat")(data=subtitle_ttml_encoded)

        moof_data = Box.build(moof)
        mdat_data = Box.build(mdat)

        return moof_data + mdat_data

    ####################################################
    #  build_subtitles_initialization
    ####################################################
    def build_subtitles_initialization(self, timescale_val: int) -> bytes:

        ftyp = Box.build(dict(type=b"ftyp",
                              major_brand=b"iso6",
                              minor_version=0,
                              compatible_brands=[b"iso6", b"dash"]))

        moov = \
            Container(type=b"moov")(children=[
                Container(type=b"mvhd")(version=0)(flags=0)(duration=0)(next_track_ID=2)(timescale=timescale_val),
                Container(type=b"mvex")(children=[
                    Container(type=b"mehd")(version=0)(flags=0)(fragment_duration=0),
                    Container(type=b"trex")(track_ID=1)
                ]),
                Container(type=b"trak")(children=[
                    Container(type=b"tkhd")(flags=3),
                    Container(type=b"mdia")(children=[
                        Container(type=b"mdhd")(creation_time=0)(modification_time=0)(timescale=timescale_val)(duration=0)(language='deu'),
                        Container(type=b"hdlr")(handler_type=b"subt")(name=b"Subtitle"),
                        Container(type=b"minf")(children=[
                            Container(type=b"dinf")(children=[
                                Container(type=b"dref")(data_entries=[Container(type=b'url ')(version=0)(flags=Container(self_contained=True))(location=None)])
                            ]),
                            Container(type=b"stbl")(children=[
                                Container(type=b"stsd")(version=1)(entries=[Container(format=b'stpp')(data_reference_index=1)(data=b'xmlns\x00\x00\x00')]),
                                Container(type=b"stts")(entries=[]),
                                Container(type=b"stsc")(entries=[]),
                                Container(type=b"stsz")(version=0)(sample_size=0)(sample_count=0)(entry_sizes=[]),
                                Container(type=b"stco")(entries=[]),
                            ]),
                            Container(type=b"sthd")(data=b'\x00\x00\x00\x00')
                        ])
                    ])
                ])
            ])

        moov_data = Box.build(moov)

        return ftyp + moov_data


####################################################
#
#  DashFragmentParser
#
####################################################
class DashFragmentParser:

    __fragment: bytes

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, fragment: bytes):

        self.__fragment = fragment
        self.__moof_box = None

    ####################################################
    #  read_ttml
    ####################################################
    def read_ttml(self) -> bytes:

        #box = Box.parse(self.__fragment)
        #print(box)

        with BufferedReader(BytesIO(self.__fragment)) as reader:

            while reader.peek(1):
                box = Box.parse_stream(reader)
                # print(box)

                if box.type == b'moof':
                    self.__moof_box = box

                if box.type == b'mdat':
                    # print(box.data)
                    return box.data

        return b''

    ####################################################
    #  update_ttml
    ####################################################
    def update_ttml(self, ttml: bytes) -> bytes:

        for traf in BoxUtil.find(self.__moof_box, b'traf'):
            for tfhd in BoxUtil.find(traf, b'tfhd'):
                tfhd.default_sample_size = len(ttml)
            for trun in BoxUtil.find(traf, b'trun'):
                for sample_info in trun.sample_info:
                    sample_info.sample_size = len(ttml)

        mdat = Container(type=b"mdat")(data=ttml)

        moof_data = Box.build(self.__moof_box)
        mdat_data = Box.build(mdat)

        return moof_data + mdat_data


####################################################
#
#  DashInitDecoder
#
####################################################
class DashInitDecoder:

    __init_file: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, init_data: bytes):

        self.__init_data = init_data

    ####################################################
    #  read_audio_sampling_rate
    ####################################################
    def read_audio_sampling_rate(self) -> int:

        with BufferedReader(BytesIO(self.__init_data)) as reader:

            while reader.peek(1):
                box = Box.parse_stream(reader)
                # print(box)

                if box.type == b'moov':
                    for trak in BoxUtil.find(box, b'trak'):
                        for mdia in BoxUtil.find(trak, b'mdia'):
                            for minf in BoxUtil.find(mdia, b'minf'):
                                for stbl in BoxUtil.find(minf, b'stbl'):
                                    for stsd in BoxUtil.find(stbl, b'stsd'):
                                        for entry in stsd.entries:
                                            if entry.format == b'mp4a':
                                                # print("entry=", entry)
                                                audio_sampling_rate = entry.sampling_rate
                                                # print(audio_sampling_rate)
                                                return audio_sampling_rate


####################################################
#
#  DashFragmentDecoder
#
####################################################
class DashFragmentDecoder:

    __fmp4_file: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, fmp4_file: str):

        self.__fmp4_file = fmp4_file

    ####################################################
    #  __sampling_rate_code
    ####################################################
    def __sampling_rate_code(self, sampling_rate: int) -> str:
        if sampling_rate == 96000:
            return '0000'
        if sampling_rate == 88200:
            return '0001'
        if sampling_rate == 64000:
            return '0010'
        if sampling_rate == 48000:
            return '0011'
        if sampling_rate == 44100:
            return '0100'
        if sampling_rate == 32000:
            return '0101'
        if sampling_rate == 24000:
            return '0110'
        if sampling_rate == 22050:
            return '0111'
        if sampling_rate == 16000:
            return '1000'
        if sampling_rate == 12000:
            return '1001'
        if sampling_rate == 11025:
            return '1010'
        if sampling_rate == 8000:
            return '1011'
        if sampling_rate == 7350:
            return '1100'

        Utils.logger_.system('DashFragmentDecoder', "DashFragmentDecoder::__sampling_rate_code unknown sampling_rate {}".format(sampling_rate))
        return '1111'

    ####################################################
    #  read_aac
    ####################################################
    def read_aac(self, wav_file: str, sampling_rate: int) -> bytes:

        sample_size = 0
        aac_data = b''

        with open(wav_file, 'wb') as file_out:
            with open(self.__fmp4_file, 'rb') as file_in:

                with BufferedReader(file_in) as reader:
                    trun_boxes = deque()

                    while reader.peek(1):
                        box = Box.parse_stream(reader)
                        #fix_headers(box)

                        for stsd_box in BoxUtil.find(box, b'stsz'):
                            sample_size = stsd_box.sample_size

                            #print("sample_size: ", sample_size)

                        if box.type == b'moof':
                            trun_boxes.extend(BoxUtil.find(box, b'trun'))
                        elif box.type == b'mdat':
                            trun_box = trun_boxes.popleft()

                            clear_box = b''

                            with BytesIO(box.data) as box_bytes:
                                for sample_info in trun_box.sample_info:

                                    #print("sample_info.sample_size: ", sample_info.sample_size)

                                    if sample_size != 0:
                                        clear_box = box_bytes.read(sample_size)
                                    else:
                                        clear_box = box_bytes.read(sample_info.sample_size)

                                    #aac adts header
                                    bits = "111111111111"  # syncword 0xFFF
                                    bits += "1"  # MPEG Version: 0 for MPEG-4, 1 for MPEG-2
                                    bits += "00"  # Layer: always 0
                                    bits += "1"  # protection absent, Warning, set to 1 if there is no CRC and 0 if there is CRC
                                    bits += "01"  # profile, the MPEG-4 Audio Object Type minus 1
                                    bits += self.__sampling_rate_code(sampling_rate)  # MPEG-4 Sampling Frequency Index (15 is forbidden)
                                    bits += "0"  # private bit, guaranteed never to be used by MPEG, set to 0 when encoding, ignore when decoding
                                    bits += "010"  # MPEG-4 Channel Configuration (in the case of 0, the channel configuration is sent via an inband PCE)
                                    bits += "0"  # originality, set to 0 when encoding, ignore when decoding
                                    bits += "0"  # home, set to 0 when encoding, ignore when decoding
                                    bits += "0"  # copyrighted id bit, the next bit of a centrally registered copyright identifier, set to 0 when encoding, ignore when decoding
                                    bits += "0"  # copyright id start, signals that this frame's copyright id bit is the first bit of the copyright id, set to 0 when encoding, ignore when decoding
                                    bits += "{0:0>13b}".format(sample_info.sample_size + 7)  # frame length, this value must include 7 or 9 bytes of header length: FrameLength = (ProtectionAbsent == 1 ? 7 : 9) + size(AACFrame)
                                    bits += "11111111111"  # Buffer fullness 0x7FF
                                    bits += "00"  # Number of AAC frames (RDBs) in ADTS frame minus 1, for maximum compatibility always use 1 AAC frame per ADTS frame

                                    hex_convert = str(hex(int(bits, 2)))
                                    #print("hex_convert: ", hex_convert)

                                    adts_header = hex_convert[2:]

                                    #print("len(clear_box)={}".format(len(clear_box)))

                                    aac_data += binascii.unhexlify(adts_header) + clear_box

                    file_out.write(aac_data)

    ####################################################
    #  read_aac_encrypted
    ####################################################
    def read_aac_encrypted(self, wav_file: str, key_str: str):

        key = binascii.unhexlify(key_str)

        sample_size = 0

        with open(self.__fmp4_file, 'rb') as file_in:

            with BufferedReader(file_in) as reader:
                senc_boxes = deque()
                trun_boxes = deque()

                while reader.peek(1):
                    box = Box.parse_stream(reader)
                    #fix_headers(box)

                    for stsd_box in BoxUtil.find(box, b'stsz'):
                        sample_size = stsd_box.sample_size

                        #print("sample_size: ", sample_size)

                    if box.type == b'moof':
                        senc_boxes.extend(BoxUtil.find(box, b'senc'))
                        trun_boxes.extend(BoxUtil.find(box, b'trun'))
                    elif box.type == b'mdat':
                        senc_box = senc_boxes.popleft()
                        trun_box = trun_boxes.popleft()

                        clear_box = b''

                        with BytesIO(box.data) as box_bytes:
                            for sample, sample_info in zip(senc_box.sample_encryption_info, trun_box.sample_info):

                                #print("sample_info.sample_size: ", sample_info.sample_size)

                                counter = Counter.new(64, prefix=sample.iv, initial_value=0)

                                cipher = AES.new(key, AES.MODE_CTR, counter=counter)

                                if sample_size != 0:
                                    cipher_bytes = box_bytes.read(sample_size)
                                    clear_box += cipher.decrypt(cipher_bytes)
                                elif not sample.subsample_encryption_info:
                                    cipher_bytes = box_bytes.read(sample_info.sample_size)
                                    clear_bytes = cipher.decrypt(cipher_bytes)
                                    clear_box += clear_bytes
                                    #print("len(clear_bytes)={}".format(len(clear_bytes)))
                                else:
                                    for subsample in sample.subsample_encryption_info:
                                        clear_box += box_bytes.read(subsample.clear_bytes)
                                        cipher_bytes = box_bytes.read(subsample.cipher_bytes)
                                        clear_box += cipher.decrypt(cipher_bytes)
                        #print("len(clear_box)={}".format(len(clear_box)))
                        box.data = clear_box
                    # out.write(Box.build(box))
            return
