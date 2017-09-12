from __future__ import absolute_import, division, print_function, unicode_literals
from pprint import pprint
import os
from lz4tools import Lz4File
import msgpack
import sys
import time
import datetime
import numpy as np
import lz4f
from enum import IntEnum, unique

__author__ = 'jleuven'


@unique
class DEBUGLEVEL(IntEnum):
    INFO = 0
    WARNING = 1
    ERROR = 2


class MsgPackWrapper(object):

    begin_time_stamp = lz4_file = unpacker = header = __max_players = __free_player_positions = __player_mapping = \
    __data = __msg_pack_data = __lz4_ctx = __output_packer = footer = None


    def __init__(self, input_file_name=None, output_file_name=None, use_iterator=True, skip_frames=5, as_numpy=False,
                 all_life_matters=False, min_life=5, debug_level=DEBUGLEVEL.ERROR, pass_through_only=False):
        """
            Constructor
            :param input_file_name: Name of input file (optional) --> openInputFile
            :param output_file_name: Name of output file (optional) --> openOutputFile
            :param use_iterator: Use class as iterator (do not pre-load into memory)
            :param skip_frames: Use one in every 'skip_frames' for output data
            :param as_numpy: Store output data in a numpy array
            :param all_life_matters: No minimal life-threshold for players
            :param min_life: minimal life-threshold for players
            :param debug_level: Debug level
            :param pass_through_only: Do not load stuff in memory. Read -> fix -> write
        """
        self.__options_dict = {"debug_level": debug_level,
                                "skip_frames": skip_frames,
                                "as_numpy": as_numpy,
                                "all_life_matters": all_life_matters,
                                "min_life": min_life}
        # self.__debug_level = debug_level
        if self.__options_dict["as_numpy"]:
            np.set_printoptions(suppress=True)
        self.__output_packer = msgpack.Packer(use_single_float=False, use_bin_type=True)
        self.__input_file_name = input_file_name
        self.__output_file_name = output_file_name
        self.__output_file = None
        self.__input_file = None
        # self.__skip_frames = skip_frames
        # self.__as_numpy = as_numpy
        # self.__all_life_matters = all_life_matters
        # self.__min_life = min_life
        self.__output_node_data = None
        self.__first_frame_timestamp = None
        if self.__input_file_name is None:
            self.done = True
        else:
            if os.path.isfile(self.__input_file_name):
                self.openInputFile(self.__input_file_name, use_iterator=use_iterator or pass_through_only)
            else:
                self.error("invalid file")
                self.done = True

        if self.__output_file_name is not None:
            self.openOutputFile(self.__output_file_name)
        if pass_through_only:
            self.passThrough()


    def __getitem__(self, item):
        """
            Make class act as a list
            :param item: Index of line in array/list
            :return: data-line
        """

        while item > len(self):
            self.next()
        if self.__options_dict["as_numpy"]:
            return self.__data[item, :]
        else:
            return self.__data[item]


    def __iter__(self):
        """
            Since a next() function is defined, this class can be used as an iterator
            :return: iterator
        """
        return self


    def __len__(self):
        """
            Make class act like list
            :return: Length of internal storage
        """
        return len(self.__data)


    def next(self):
        """
            With this function the class can be used as an iterator
            :return: data-line
        """
        if self.done:
            self.warning("Asking for next frame after done")
            # Itterator stuff
            raise StopIteration

        try:
            msg_pack_frame = None
            for _ in range(self.__options_dict["skip_frames"]):  # Skip x frames before we return 1
                msg_pack_frame = self.__getNextMsgPackFrame()

            # Correct frame found, proccess it.
            new_frame = self.__processFrame(msg_pack_frame)

            # Add new frame to internal storage (so it can be reused, without reading from disk)
            self.__data.append(new_frame)
            self.info("NEXT Complete")
            return new_frame
        except EOFError:
            # Itterator stuff
            self.done = True
            raise StopIteration


    def setOption(self, option_name, value):
        """
            Function to set/change option after init
            :param option_name: Name of option to be set
            :param value: Value option needs to be set to.
            :return: None
        """
        if option_name in self.__options_dict:
            self.__options_dict[option_name] = value
        else:
            self.error("unknown option")


    def getOption(self, option_name):
        """
            Get current value of option
            :param option_name: Name of option to get
            :return: value of option
        """
        if option_name in self.__options_dict:
            return self.__options_dict[option_name]
        else:
            return self.error("unknown option")


    def generateEmptyOutputNodeData(self, num_output_values=1):
        if self.__options_dict["as_numpy"]:
            self.__output_node_data = np.zeros((len(self.__data), num_output_values))


    def setOutputNodeData(self, from_timestamp, to_timestamp, values):
        if self.__output_node_data is None:
            self.generateEmptyOutputNodeData()
        from_timestamp_with_offset = self.__determineTimestampOffset(from_timestamp)
        to_timestamp_with_offset = self.__determineTimestampOffset(to_timestamp)

        selection = self.__getSelection(from_timestamp=from_timestamp_with_offset, to_timestamp=to_timestamp_with_offset)

        self.__output_node_data[selection] = values


    def generateOutputNodeDataFromCSV(self, csv_file_name):

        if csv_file_name is None:
            self.error("No file specified".format(csv_file_name))
        abs_csv_file_name = os.path.abspath(csv_file_name)
        if not os.path.isfile(abs_csv_file_name):
            self.error("No such file or directory {}".format(abs_csv_file_name))

        text = open(csv_file_name, 'rb').read().replace(';\r\n', '\n')
        import pandas as pd
        from StringIO import StringIO
        temp_file = StringIO()
        temp_file.write(text)
        temp_file.seek(0)

        csv_file = pd.DataFrame.from_csv(temp_file, sep=",", parse_dates=True)
        self.info(csv_file[["StartTime", "EndTime"]])

        from dateutil import parser
        for row_idx, row in csv_file[["StartTime", "EndTime"]].iterrows():
            start_time = parser.parse(row[0])
            end_time = parser.parse(row[1])
            self.setOutputNodeData(from_timestamp=start_time, to_timestamp=end_time, values=1)

        return self.__output_node_data


    def info(self, message=""):
        if self.__options_dict["debug_level"] <= DEBUGLEVEL.INFO:
            print("INFO: {}".format(message))


    def warning(self, message=""):
        if self.__options_dict["debug_level"] <= DEBUGLEVEL.ERROR:
            print("WARNING: {}".format(message))


    def error(self, message=""):
        if self.__options_dict["debug_level"] <= DEBUGLEVEL.ERROR:
            raise Exception("ERROR: {}".format(message))


    def openInputFile(self, input_file_name, use_iterator=True):

        self.__input_file_name = input_file_name

        if self.__input_file_name is None:
            self.warning("Empty input_file_name")
        elif not os.path.isfile(self.__input_file_name):
            self.warning("File does not exist: {}".format(self.__input_file_name))

        if self.__input_file is not None:
            self.closeInputFile()
        self.info("opening {}".format(self.__input_file_name))
        self.__input_file = open(self.__input_file_name, "rb")

        self.info("creating lz4")

        self.lz4_file = Lz4File("input", self.__input_file)
        self.info("creating unpacker")
        self.unpacker = msgpack.Unpacker(self.lz4_file, use_list=False)
        self.done = False

        self.info("open header")
        self.header = self.unpacker.next()
        self.__fixHeader()

        self.__max_players = self.header["maxPlayers"]
        self.__free_player_positions = range(self.__max_players)
        self.__player_mapping = []
        self.__data = []
        self.__msg_pack_data = []
        try:
            self.begin_time_stamp = datetime.datetime.fromtimestamp(time.mktime(time.strptime(os.path.split(self.__input_file_name)[-1].replace("_PlayerData.lz4", "").replace("_PlayerData-selection.lz4", "")[:-1], "%Y_%m_%d-%H.%M.%S.%f")))
        except:
            self.begin_time_stamp = None
        self.info(self.header)
        if not use_iterator:
            self.getAllFrames()


    def closeInputFile(self, load_remaining=False):
        if load_remaining:
            self.getAllFrames()
        self.__input_file.close()
        self.done = True
        self.__first_frame_timestamp = None


    def openOutputFile(self, output_file_name):
        if self.__output_file is not None:
            self.closeOutputFile()
        # self.__lz4_ctx = lz4f.createCompContext()
        self.info("opening {} for writing".format(output_file_name))
        self.__output_file = open(output_file_name, "wb")
        # self.output_file.write(lz4f.compressBegin(self.__lz4_ctx))


    def closeOutputFile(self):
        if self.__output_file is not None:
            # self.output_file.write(lz4f.compressEnd(self.__lz4_ctx))
            self.__output_file.flush()
            self.__output_file.close()
            self.__output_file = None


    def savez(self, npz_output_file_name="output.npz"):
        """
            Store internal data to numpy file
            :param npz_output_file_name: File name to save to
            :return: None
        """
        with open(str(npz_output_file_name), str("w")) as output_file:
            np.savez_compressed(output_file, data=self.__data)


    def loadz(self, npz_input_file_name="", append_new_data=False):
        """
            Load numpy file into internal memory
            :param npz_input_file_name: File name of file to load
            :param append_new_data: Allow adding of new data
            :return: None
        """
        if os.path.isfile(npz_input_file_name):
            with open(str(npz_input_file_name), str("r")) as input_file:
                self.__data = np.load(input_file)["data"]
                self.__options_dict["as_numpy"] = True  # Data is no completely numpy
                self.done = not append_new_data  # No more reading... (may need)
        else:
            raise Exception("No such file")


    def getAllFrames(self):
        """
            Use te self iterator to load all the data
            :return: numpy array or list of lists
        """
        for _ in self:
            pass

        if self.__options_dict["as_numpy"]:
            return np.vstack(self.__data)
        else:
            return self.__data


    def getInputNodeLength(self):
        return self.__max_players * 3 + 3


    def getInputNodeData(self):
        if self.__options_dict["as_numpy"]:
            data = np.vstack(self.__data)
            return data[:, :-1]


    def getMsgPackFrame(self, item):
        """
            Get a specific msg-pack-frame
            :param item: index for frame
            :return: frame
        """
        if item < len(self.__msg_pack_data):
            return self.__msg_pack_data[item]
        else:
            self.error("Item: {} not loaded in msg-pack-data".format(item))


    def writeMsgPackFrameSelectionMulti(self, output_file_name=None, from_timestamps=None, to_timestamps=None):
        """
            Write multiple selections from data to file
            :param output_file_name: Optional output file_name
            :param from_timestamps: list of beginning timestamps
            :param to_timestamps: list of ending timestamps
            :return: None
        """
        if isinstance(from_timestamps, list) and isinstance(to_timestamps, list):
            for from_timestamp, to_timestamp in zip(from_timestamps, to_timestamps):
                self.writeMsgPackFrameSelectionSingle(output_file_name=output_file_name,
                                                      from_timestamp=from_timestamp,
                                                      to_timestamp=to_timestamp)


    def writeMsgPackFrameSelectionSingle(self, output_file_name=None, from_timestamp=None, to_timestamp=None):
        """
            Write selection of data to output file
            :param output_file_name: Optional output file name
            :param from_timestamp: Timestamp at the beginning of selection
            :param to_timestamp: Timestamp at the end of the selection
            :return: None
        """

        if self.begin_time_stamp is None:
            return False

        # Create temporary file
        import StringIO
        temp_output_file = StringIO.StringIO()

        # Convert timestamps
        from_timestamp_with_offset = self.__determineTimestampOffset(from_timestamp)
        to_timestamp_with_offset = self.__determineTimestampOffset(to_timestamp)
        if from_timestamp_with_offset is None:
            from_timestamp_with_offset = 0
        if to_timestamp_with_offset is None:
            to_timestamp_with_offset = np.inf

        # Determine output-filename
        if output_file_name is None:
            output_file_name = self.__input_file_name.replace(".lz4", "-selection.lz4")

        # If no output file -> open it
        if self.__output_file is None:
            self.openOutputFile(output_file_name=output_file_name)

        # Write msg-pack-header
        temp_output_file.write(self.__output_packer.pack(self.header))

        # Write all frames within selection
        for frame in self.__msg_pack_data:
            if from_timestamp_with_offset < frame["timeStamp"] < to_timestamp_with_offset:
                temp_output_file.write(self.__output_packer.pack(frame))
                # compressed_string = lz4f.compressUpdate(self.__output_packer.pack(frame), self.__lz4_ctx)
                # self.output_file.write(compressed_string)

        # If it exists: write footer
        if self.footer is not None:
            temp_output_file.write(self.__output_packer.pack(self.footer))


        temp_output_file.seek(0)
        self.__output_file.write(lz4f.compressFrame(temp_output_file.read()))


    def passThrough(self):
        """
            Function to load skip loading all data into memory
            :return:
        """
        if self.__input_file is None or self.__output_file is None:
            self.error("No input or outputfile opened")
            return False

        # Temporary file
        import StringIO
        temp_output_file = StringIO.StringIO()

        # Write header
        packed_header = self.__output_packer.pack(self.header)
        temp_output_file.write(packed_header)
        frame_count = 0

        # Write all frames
        while True:
            try:
                msg_pack_frame = self.__getNextMsgPackFrame(save_data=False)
                temp_output_file.write(self.__output_packer.pack(msg_pack_frame))
                frame_count += 1
            except EOFError:
                self.info("DONE. Frames: {}".format(frame_count))
                break

        # If it exists -> write footer
        if self.footer is not None:
            temp_output_file.write(self.__output_packer.pack(self.footer))

        # Write temporary file through lz4 compression
        temp_output_file.seek(0)
        self.__output_file.write(lz4f.compressFrame(temp_output_file.read()))

        self.closeOutputFile()
        return True


    def __determineTimestampOffset(self, time_stamp):
        """
            Convert datetime to integer notation
            :param time_stamp: timestamp
            :return: converted timestamp
        """
        if isinstance(time_stamp, datetime.datetime):
            # time_stamp = time_stamp - self.begin_time_stamp
            # print(self.header["startDateTime"])
            if self.begin_time_stamp is None:
                time_stamp = time_stamp - datetime.datetime(1970, 1, 1)
                time_stamp = time_stamp.total_seconds() - self.header["startDateTime"]
            else:
                time_stamp = time_stamp - self.begin_time_stamp
                time_stamp = time_stamp.total_seconds()


        return time_stamp


    def __fixHeader(self):
        """
            Fix header (if needed)
            :return: fixed header
        """
        if "startDateTime" not in self.header:
            self.header["startDateTime"] = self.__getStartDateTime()
        if "hostname" not in self.header:
            self.header["hostname"] = str(self.__getHostName())
        if "PTZPosition" not in self.header:
            self.header["PTZPosition"] = self.__getPTZPosition()
        return self.header


    def __fixPlayers(self, frame):
        """
            Fix players in frame (if needed)
            :param frame: input frame
            :return: fixed frame
        """
        new_players = []
        players_converted = False
        for player in frame["players"]:
            if isinstance(player, (list, tuple)):
                players_converted = True
                new_players.append({"normPosition": player, "weight": 1})
        if players_converted:
            frame["players"] = new_players
            self.info("Players converted")
        return frame


    def __fixBallLines(self, frame):
        new_ballLines = []
        ballLines_converted = False
        for ballLine in frame["ballLines"]:
            if isinstance(ballLine, list):
                ballLines_converted = True
                new_ballLines.append({"normPosition": ballLine,
                                      'probability': 1})
        if ballLines_converted:
            frame["ballLines"] = new_ballLines
            self.info("ballLines converted")


    def __fixTimeStamp(self, frame):
        """
            Fix timestamp (if needed)
            :param frame: input frame to fix timestamp for
            :return: fixed frame
        """
        if self.__first_frame_timestamp is None:
            self.__setFirstFrameTimeStamp(frame)
        if frame["timeStamp"] >= self.__first_frame_timestamp:
            frame["timeStamp"] = frame["timeStamp"] - self.__first_frame_timestamp
        else:
            self.info("{} {}".format(frame["timeStamp"], self.__first_frame_timestamp))
        return frame


    def __freePlayer(self, index):
        """
            Player with mapping index is declared free. Append it to the list of free mappings
            :param index:
            :return: None
        """
        if index is None:  # This should not be happening
            self.error("Trying to free a non-existent player mapping")
        self.__free_player_positions.append(index)


    def __getHostName(self):
        """
            Get hostname (for header fix)
            :return: Hostname
        """
        split_file_name = self.__input_file_name.split(os.path.sep)
        host_name = "unkown"
        for file_name_part in split_file_name:
            if ".vpn" in file_name_part:
                host_name = file_name_part.replace(".vpn", "")
        return host_name


    def __getNextMsgPackFrame(self, save_data=True):
        """
            Helper function to get next msg-pack-frame
            :param save_data: Boolean option to save data to internal structure
            :return: msg-pack-frame (fixed)
        """
        msg_pack_frame = self.unpacker.next()
        if "endLogTime" in msg_pack_frame:
            self.footer = msg_pack_frame
            return self.footer
        msg_pack_frame = self.__fixTimeStamp(msg_pack_frame)
        msg_pack_frame = self.__fixPlayers(msg_pack_frame)
        msg_pack_frame = self.__fixBallLines(msg_pack_frame)

        if self.__first_frame_timestamp is None:
            self.__setFirstFrameTimeStamp(msg_pack_frame)
        if save_data:
            self.__msg_pack_data.append(msg_pack_frame)
        return msg_pack_frame


    def __getPTZPosition(self):
        """
            Get ptz-position for in header
            :return: ptz-position or default ptz-position
        """
        try:
            import requests
            get_response = requests.get("http://{}.vpn:9999/world/moduleCam1".format(self.header["hostname"]), timeout=2).json()["response"]

            post_response = requests.post("http://localhost:9999/command/world/moduleCam1", json={"calibrate": get_response["calibrate"]}, timeout=2).json()["response"]
            return post_response["calibrate"]["position"]
        except Exception as e:
            self.warning(e)
            return [0.0, -37.5, 10.0]


    def __getStartDateTime(self):
        """
            Get startTime from input_file_name
            :return: start_time (integer format)
        """
        input_file_name_without_path = os.path.split(self.__input_file_name)[-1]
        # We assume that the first 24 characters contain a timestamp
        time_stamp_in_file_name = input_file_name_without_path[:24]
        try:
            return int(time.mktime(time.strptime(time_stamp_in_file_name, "%Y_%m_%d-%H.%M.%S.%f")))
        except Exception as e:
            self.warning(unicode(e))
            return 0


    def __nextFreePlayerPosition(self):
        """
            Wrapper function to shield the list with free array indices
            :return: index of empty array spot
        """
        return_value = self.__free_player_positions.pop(0)
        return return_value


    def __processFrame(self, input_frame):
        """
            Convert msg-pack frame to list/array for fast proccessing
            :param input_frame: msg-pack frame
            :return: list/array
        """
        if input_frame is None:
            return

        # Create empty array/list
        if self.__options_dict["as_numpy"]:
            return_frame = np.zeros(((self.__max_players * 3) + 4))
        else:
            return_frame = [0] * ((self.__max_players * 3) + 4)


        # Remove players from previous frame that were deleted in virtcam
        for remove_index in input_frame["playersRemovedIndices"]:
            if remove_index == -1:  # Special index to remove all
                raise Exception("remove all")
            elif remove_index >= (self.__max_players - len(self.__free_player_positions) - 1):
                # We don't care about players that were removed that only existed in current frame
                continue
            else:
                # Release mapping for current player
                self.__freePlayer(self.__player_mapping.pop(remove_index)[0])

        # Extract data for players
        for player_idx, player in enumerate(input_frame["players"]):
            if player_idx == len(self.__player_mapping):  # No mapping for this player exist -> create it
                self.__player_mapping.append([self.__nextFreePlayerPosition(), 0])
            elif player_idx > len(self.__player_mapping):  # This can only happen when a player is skipped...
                raise Exception("This should not be happening more players are in mapping")

            # Get mapping
            player_mapping_idx, life = self.__player_mapping[player_idx]

            # Increase life
            self.__player_mapping[player_idx][1] = life + 1

            # Either a player should have a minimal life (life > min_life) or we don't care how old a player is
            self.info(player)
            if life > self.__options_dict["min_life"] or self.__options_dict["all_life_matters"]:
                return_frame[player_mapping_idx * 3 + 0] = player["normPosition"][0]
                return_frame[player_mapping_idx * 3 + 1] = player["normPosition"][1]
                return_frame[player_mapping_idx * 3 + 1] = player["weight"]


        # Extract ball/ballLine data (currently only main ballLine or first ball)
        if "ballLines" in input_frame:
            return_frame[self.__max_players * 2] = input_frame["ballLines"][input_frame["mainBall"]][0]
            return_frame[self.__max_players * 2 + 1] = input_frame["ballLines"][input_frame["mainBall"]][1]
            return_frame[-1] = input_frame["timeStamp"]
        else:
            return_frame[self.__max_players * 2] = input_frame["balls"][0][0]
            return_frame[self.__max_players * 2 + 1] = input_frame["balls"][0][1]
            return_frame[self.__max_players * 2 + 2] = input_frame["balls"][0][3]
            return_frame[-1] = input_frame["timeStamp"]

        return return_frame


    def __getSelection(self, from_timestamp=None, to_timestamp=None):
        from_timestamp_with_offset = self.__determineTimestampOffset(from_timestamp)
        to_timestamp_with_offset = self.__determineTimestampOffset(to_timestamp)

        if isinstance(self.__data, list):
            temp_data = np.vstack(self.__data)
        else:
            temp_data = np.copy(self.__data)

        if from_timestamp is not None and to_timestamp is not None:
            select = np.where((temp_data[:, -1] > from_timestamp_with_offset) &
                              (temp_data[:, -1] < to_timestamp_with_offset))
        elif from_timestamp is not None:  # only a beginning
            select = np.where((temp_data[:, -1] > from_timestamp_with_offset))
        elif to_timestamp is not None:  # only an end
            select = np.where((temp_data[:, -1] < to_timestamp_with_offset))
        else:  # No selection given -> return all
            select = (range(len(self)),)

        return select


    def __selectData(self, from_timestamp=None, to_timestamp=None):
        """
            Make a selection based on timestamp
            :param from_timestamp: datetime/seconds
            :param to_timestamp: datetime/seconds
            :return: Data between from_timestamp and to_timestamp
        """
        select = self.__getSelection(from_timestamp=from_timestamp, to_timestamp=to_timestamp)

        return self.__data[select[0], :-1]


    def __setFirstFrameTimeStamp(self, frame):
        """
            If the timestamp from the first playerDataLine has not been saved yet -> save it
            :param frame: Frame to save timestamp from
            :return: None
        """
        if self.__first_frame_timestamp is not None and self.__debug_level:
            self.warning("Warning overwriting first frame")
        self.__first_frame_timestamp = frame["timeStamp"]


    @property
    def all_data(self):
        """
            Wrapper for internal data.
            :return: list/numpy-arrays
        """
        if self.__options_dict["as_numpy"] and isinstance(self.__data, list):
            return np.vstack(self.__data)
        else:
            return self.__data



