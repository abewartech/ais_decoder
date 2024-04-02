#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
"""
import datetime
import json
import logging
import re
import traceback
import os

import ais

log = logging.getLogger("main.ais_decode")
# log.setLevel('DEBUG')


class AIS_Decoder:
    def __init__(self, ais_message_format):
        self.ais_format = ais_message_format

    def eta_from_multi(self, decoded_dict, event_time):
        # Turn the ETA items in Voyage reports into a datetime object
        try:
            event_timestamp = datetime.datetime.fromtimestamp(int(event_time))
            eta_datetime = datetime.datetime(
                year=event_timestamp.year,
                month=decoded_dict["eta_month"],
                day=decoded_dict["eta_day"],
                hour=decoded_dict["eta_hour"],
                minute=decoded_dict["eta_minute"],
            ).isoformat()
        except Exception as err:
            # generally the eta's have 60 minutes or day 0 or something
            #   like that
            log.debug(err)
            eta_datetime = None
        return eta_datetime

    def parse_decode(self, line):
        log.debug("Parsing: {0}".format(line))
        parsed_line = {}
        (
            parsed_line["rx_time"],
            parsed_line["meta"],
            parsed_line["ais"],
        ) = re.split(r": \\|\\", line)
        parsed_line["meta"], parsed_line["meta_checksum"] = re.split(
            r"\*", parsed_line["meta"]
        )
        time_found = re.search(r"[0-9]{10}", parsed_line["meta"])
        if time_found:
            parsed_line["event_time"] = time_found.group(0)
        else:
            parsed_line["event_time"] = None
        (
            parsed_line["talker"],
            parsed_line["frag_count"],
            parsed_line["frag_num"],
            parsed_line["seq_id"],
            parsed_line["radio_chan"],
            parsed_line["payload"],
            parsed_line["padding"],
            parsed_line["checksum"],
        ) = re.split(r",|\*", parsed_line["ais"])

        log.debug("Parsed: {0}".format(parsed_line))
        return parsed_line

    def single_decode(self, parsed_line):
        decoded_line = self.ais_decode(parsed_line)

        # Clean up Trailing @'s
        if "callsign" in decoded_line:
            decoded_line["callsign"] = decoded_line["callsign"].rstrip("@")
        if "name" in decoded_line:
            decoded_line["name"] = decoded_line["name"].rstrip("@")
        if "destination" in decoded_line:
            decoded_line["destination"] = decoded_line["destination"].rstrip(
                "@"
            )

        return decoded_line

    def multi_decode(self, parsed_line, next_line):
        log.debug("Multiline")
        parsed_line["payload"] = parsed_line.get("payload") + next_line.get(
            "payload"
        )
        parsed_line["padding"] = next_line["padding"]
        decoded_line = self.ais_decode(parsed_line)

        # Clean up Trailing @'s
        if "callsign" in decoded_line:
            decoded_line["callsign"] = decoded_line["callsign"].rstrip("@ ")
        if "name" in decoded_line:
            decoded_line["name"] = decoded_line["name"].rstrip("@ ")
        if "destination" in decoded_line:
            decoded_line["destination"] = decoded_line["destination"].rstrip(
                "@ "
            )

        # Turn eta Columns into datetime.
        if "eta_month" in decoded_line:
            decoded_line["eta"] = self.eta_from_multi(
                decoded_line, parsed_line["event_time"]
            )

        log.debug("Line Decoded: {0}".format(decoded_line))
        return decoded_line

    def ais_decode(self, parsed_line):
        # This  is the function that justifies this service.
        # Since the Ais-i-mov was refactor just adding this
        #   would complete the decoding in the previous service
        #   rendering this one obsolute.
        log.debug("Decoding: {0}".format(parsed_line))
        try:
            decode_dict = ais.decode(
                parsed_line["payload"], int(parsed_line["padding"])
            )
        except Exception as err:
            log.debug(err)
            try:
                padding = 71 - len(parsed_line["payload"])
                decode_dict = ais.decode(parsed_line["payload"], padding)
            except Exception as err:
                decode_dict = {}
                log.debug(err)
        return decode_dict

    def message_processor(self, rabbit_msg):
        """
        Take the message, parse it and decode it. return a decoded dict.
        {"server_time": "2020-12-04T14:43:01.461071",
        "event_time": "",
        "routing_key": "sink.key.test",
        "multiline": false,
        "message": "!ABVDM,1,1,,B,34SH0b0OiQ1D52cd=AJli3tb0000,0*44\r"}
        """
        log.debug("Parsing MSG: " + str(rabbit_msg.body))
        udm_dict = json.loads(rabbit_msg.body)
        try:
            multimsg = udm_dict.get("multiline")
            decoder = Basic_AIS()
            if multimsg is False:
                # Single Line Messages, the bulk of 'em
                parsed_line = decoder.return_dict(udm_dict["ais"])
                udm_dict["parsed_msg"] = parsed_line
                decoded_line = self.single_decode(parsed_line)
                udm_dict["decoded_msg"] = decoded_line
            elif multimsg is True:
                # The rare multiline message. Already grouped by AIS-i-mov
                udm_dict["parsed_msg"] = {}
                parsed_list = []
                for msg in udm_dict["ais"]:
                    parsed_line = decoder.return_dict(msg)
                    parsed_list.append(parsed_line)

                if len(parsed_list)> 2:
                    log.warn("TODO -- decoding of multiline messages > 3 not currently supported")

                # for now we just keep the first two parts
                decoded_line = self.multi_decode(parsed_list[0], parsed_list[1])
                
                udm_dict["parsed_msg"] = {f"parsed_msg{ '_'+str(ndx) if ndx > 0 else '' }": line for ndx, line in enumerate(parsed_list) } 
                udm_dict["decoded_msg"] = decoded_line
            else:
                log.warning("Unrecognized message: " + str(udm_dict))
                udm_dict["decoded_msg"] = {}

            # Message Decoded. Change routing key:
            udm_dict['routing_key'] = os.getenv('SNK_PRODUCE_KEY')
            log.debug("Decoded :" + str(udm_dict))
        except Exception as e:
            log.warning(
                "Problem with parsing and decoding line: {0}".format(udm_dict)
            )
            log.warning(traceback.format_exc())
            log.error(e)
        return udm_dict


class Basic_AIS:
    # The most basic AIS class
    # This is for a AIS source that has no header/footers, and no other
    # meta-data:
    # "!ABVDM,1,1,,B,34SH0b0OiQ1D52cd=AJli3tb0000,0*44\r"
    def __init__(self):
        self.reset()

    def reset(self):
        self.header = None
        self.footer = None
        self.payload = None
        self.event_time = None
        self.talker = None
        self.frag_count = None
        self.frag_num = None
        self.seq_id = None
        self.radio_chan = None
        self.payload = None
        self.padding = None
        self.checksum = None

    def parse(self, ais_msg):
        log.debug("Parsing plain AIS message: " + str(ais_msg))
        (
            self.talker,
            self.frag_count,
            self.frag_num,
            self.seq_id,
            self.radio_chan,
            self.payload,
            self.padding,
            self.checksum,
        ) = re.split(r",|\*", ais_msg)
        self.checksum = self.checksum[0:2]

    def return_dict(self, ais_msg):
        self.parse(ais_msg)
        parsed_dict = {
            "header": self.header,
            "footer": self.footer,
            "event_time": self.event_time,
            "talker": self.talker,
            "frag_count": self.frag_count,
            "frag_num": self.frag_num,
            "seq_id": self.seq_id,
            "radio_chan": self.radio_chan,
            "payload": self.payload,
            "padding": self.padding,
            "checksum": self.checksum,
        }
        return parsed_dict
