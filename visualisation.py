from __future__ import absolute_import, division, print_function, unicode_literals
from pprint import pprint
import os
os.environ["DISPLAY"] = ":0"

import msgpack
from lz4tools import Lz4File
import visual as vis
import traceback
import sys
import time
import datetime
from enum import Enum
import math

__author__ = 'jleuven'


class Colours(tuple, Enum):
    # member_type = tuple
    white  = (255 / 255, 255 / 255, 255 / 255)
    black  = (  0 / 255,   0 / 255,   0 / 255)
    green  = ( 40 / 255, 180 / 255,  99 / 255)
    red    = (146 / 255,  43 / 255,  33 / 255)
    blue   = ( 40 / 255, 116 / 255, 166 / 255)
    gray   = (128 / 255, 128 / 255, 128 / 255)
    yellow = (233 / 255, 233 / 255,  61 / 255)
    purple = (186 / 255, 61  / 255, 233 / 255)


class Goal(object):

    def __init__(self, pos):
        self.dimensions = (1.2, 2.14, 3.66)
        self.pos = (pos[0] + self.dimensions[0] / 2
                    if pos[0] > 0 else
                    pos[0] - self.dimensions[0] / 2,
                    self.dimensions[1] / 2,
                    0)
        self.box = vis.box(pos=self.pos, length=self.dimensions[0], width=self.dimensions[2], height=self.dimensions[1], color=Colours.gray)

class SemiCircle(object):


    def __init__(self, radius, left, field_dimensions, dotted=False):
        if left:
            x_offset = -field_dimensions[0]
        else:
            x_offset = field_dimensions[0]


class Ball(object):

    def __init__(self, pos, value, radius=0.3):
        self.radius = radius
        self.pos = (pos[0], self.radius, pos[1])
        self.value = value
        self.sphere = vis.sphere(pos=self.pos, radius=self.radius, color=Colours.red)


    def setPos(self, pos, value):
        self.pos = (pos[0], self.radius, -pos[1])
        self.value = value
        self.sphere.pos = self.pos
        if self.value > 1:
            self.unhide()
        else:
            self.hide()


    def setMain(self):
        self.sphere.color = Colours.yellow


    def unsetMain(self):
        self.sphere.color = Colours.red


    def hide(self):
        self.sphere.visible = False


    def unhide(self):
        self.sphere.visible = True


    def __del__(self):
        self.hide()


class Player(object):

    def __init__(self, idx, pos, field_dimensions, player_length):
        self.idx = idx
        self.field_dimensions = field_dimensions
        self.length = player_length
        self.pos = (pos[0] * self.field_dimensions[0], pos[1] * self.field_dimensions[1])

        self.label_offset_x = 0
        self.label_offset_y = 3
        self.label = vis.label(text=str(idx))
        self.life = 0
        self.alive = 5
        self.cone = vis.cylinder(pos=(self.pos[0], 0, self.pos[1]), axis=(0, self.length, 0), radius=0.5,
                                 color=Colours.blue if self.life < self.alive else Colours.purple)
        self.__setLabelPos()


    def __setLabelPos(self):
        self.label.pos = (self.pos[0] + self.label_offset_x, self.label_offset_y, self.pos[1] + self.label_offset_x)


    def __setConePos(self):
        self.cone.pos = (self.pos[0], 0, self.pos[1])


    def setPos(self, pos):
        self.life += 1
        if self.life >= self.alive:
            self.cone.color = Colours.blue
        else:
            self.cone.color = Colours.purple
        self.pos = (pos[0] * self.field_dimensions[0], -pos[1] * self.field_dimensions[1])
        self.__setConePos()
        self.__setLabelPos()


    def hide(self):
        self.cone.visible = False
        self.label.visible = False


    def unhide(self):
        self.cone.visible = True
        self.label.visible = True


    def __del__(self):
        self.hide()
        del self.cone
        del self.label


class Camera(object):

    def __init__(self, scene):

        pass



class Visualiser(object):
    input_file_name = input_file = lz4_file = unpacker = header = field_dimensions = player_length = scene = \
        frame_number = __players = __balls = last_player_index = frame_rate = render_players = render_balls = \
        field = middle_line = field_edge = left_23 = right_23 = circle = left_circle = right_circle = \
        left_circle_dotted = right_circle_dotted = left_dot = middle_dot = right_dot = None

    def __init__(self, filename=None):
        self.input_file_name = filename
        file_name = os.path.split(self.input_file_name)[-1]
        print(file_name)
        self.begin_time_stamp = file_name.replace("_PlayerData.lz4", "")[:-1]
        import time
        import datetime
        # parser.parse(file_name.replace("_PlayerData.lz4", "")[:-1])
        self.begin_time_stamp = datetime.datetime.fromtimestamp(time.mktime(time.strptime(file_name.replace("_PlayerData.lz4", "")[:-1], "%Y_%m_%d-%H.%M.%S.%f")))
        print(self.begin_time_stamp)
        if filename is not None:
            self.input_file = open(filename, "rb")

            self.lz4_file = Lz4File("a", self.input_file)
            self.unpacker = msgpack.Unpacker(self.lz4_file)
            self.header = self.unpacker.next()
            self.field_dimensions = self.header["fieldDimensions"]
            self.player_length = self.header["playerEstimateLength"]
            self.scene = vis.display(width=1920, height=1080)
            # self.scene.forward = (0, -0.5, -1)
            self.expected_frame_rate = 30
            self.scene.autocenter = False
            self.frame_number = 1
            self.__players = []
            self.__balls = []
            self.last_player_index = 0
            self.frame_rate = 30
            self.render_players = self.render_balls = True
            self.disableManualControl()
            self.begin_file_time_stamp = None
        else:
            self.input_file = None


    def enableManualControl(self):
        self.scene.userspin = True
        self.scene.userzoom = True


    def disableManualControl(self):
        self.scene.userspin = False
        self.scene.userzoom = False

    def toggleManualControl(self):
        if self.scene.userspin == False:
            self.enableManualControl()
        else:
            self.disableManualControl()


    def startSimulation(self, begin_time_stamp=None, end_time_stamp=None):
        self.drawField()
        self.drawCircles()
        self.drawDots()

        while True:
            vis.rate(self.frame_rate)
            if self.scene.kb.keys:
                self.handleKeys()
            try:

                cur_frame = self.unpacker.next()
                if self.begin_file_time_stamp is None:
                    self.begin_file_time_stamp = cur_frame["timeStamp"]
                time_diff = cur_frame["timeStamp"] - self.begin_file_time_stamp
                cur_time_stamp = self.begin_time_stamp + datetime.timedelta(seconds=self.frame_number / self.expected_frame_rate)
                cur_time_stamp_from_offset = self.begin_time_stamp + datetime.timedelta(seconds=round(time_diff, 2))
                self.frame_rate = (self.expected_frame_rate / max(1, ((cur_time_stamp_from_offset - self.begin_time_stamp).total_seconds() / (cur_time_stamp - self.begin_time_stamp).total_seconds())))
                self.score_board.text = str("FrameNumber: {}".format(self.frame_number)) + "\n" + \
                                        str("Time since start: ") + str("%0.2f" % time_diff) + "\n" + \
                                        str("Time from timestamps: ") + str('{:%Y-%m-%d %H:%M:%S}'.format(cur_time_stamp_from_offset if cur_frame["timeStamp"] != 0.0 else cur_time_stamp)) + "\n" + \
                                        str("Time from framenumber: ") + str('{:%Y-%m-%d %H:%M:%S}'.format(cur_time_stamp))  + "\n" + \
                                        str("Time skew: ") + str("%0.2f" % self.frame_rate)

                while begin_time_stamp is not None and begin_time_stamp < cur_time_stamp > end_time_stamp:

                    self.frame_number += 1
                    cur_time_stamp = self.begin_time_stamp + datetime.timedelta(seconds=self.frame_number / 27)
                    cur_frame = self.unpacker.next()

                self.drawPlayers(cur_frame["players"], cur_frame["playersRemovedIndices"])
                if "ballLines" in cur_frame:
                    self.drawBallLines(cur_frame["ballLines"], cur_frame["ballLineValues"], cur_frame["mainBall"])
                else:
                    self.drawBalls(cur_frame["balls"])
                self.frame_number += 1
                # print(self.begin_time_stamp + datetime.timedelta(seconds=self.frame_number/30))
            except Exception as e:
                pprint(e)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback)
                break


    def hidePlayers(self):
        for player in self.__players:
            player.hide()


    def handleKeys(self):
        while self.scene.kb.keys:
            key = self.scene.kb.getkey()

            if key == "[":
                self.frame_rate = max(5, self.frame_rate - 5)
            elif key == "]":
                self.frame_rate += self.frame_rate + 5
            elif key == "\\":
                self.frame_rate = 30
            elif key == "b":
                self.render_balls = not self.render_balls
            elif key == "p":
                self.render_players = not self.render_players
            elif key == "m":
                self.toggleManualControl()
            elif key == "v":
                self.visualisation = not self.visualisation


    def drawPlayers(self, input_players, remove_player_indices):
        for remove_player_index in remove_player_indices:
            # if remove_player_index < len(remove_player_indices):
            try:
                remove_player = self.__players.pop(remove_player_index)
                remove_player.visible = False
                del remove_player
            except:
                pass

        for player_nr, player in enumerate(input_players):
            self.drawPlayer(player, player_nr)


    def drawPlayer(self, pos, idx=None):
        if idx >= len(self.__players):
            self.__players.append(Player(idx=self.last_player_index, pos=pos, field_dimensions=self.field_dimensions, player_length=self.player_length))
            self.last_player_index += 1
        else:
            self.__players[idx].setPos(pos=pos)

        if self.render_players:
            self.__players[idx].unhide()
        else:
            self.__players[idx].hide()


    def drawBalls(self, input_balls, main_ball=0):
        balls = []
        values = []
        for ball in input_balls:
            balls.append(ball[0:2])
            values.append(ball[3])
        self.drawBallLines(balls, values, main_ball)


    def drawBallLines(self, input_balls, ball_values, main_ball):
        valid_balls = []

        for ball in input_balls:
            if -self.field_dimensions[0] < ball[0] < self.field_dimensions[0] and -self.field_dimensions[1] < ball[1] < self.field_dimensions[1]:
                valid_balls.append(ball)

        while len(valid_balls) < len(self.__balls):
            remove_ball = self.__balls.pop()
            del remove_ball

        for ball_nr, ball in enumerate(valid_balls):
            if self.render_balls:
                if ball_nr < len(self.__balls):
                    self.__balls[ball_nr].setPos(pos=ball, value=ball_values[ball_nr])
                else:
                    self.__balls.append(Ball(pos=ball, value=ball_values[ball_nr]))
                if main_ball == ball_nr:
                    self.__balls[ball_nr].setMain()
                else:
                    self.__balls[ball_nr].unsetMain()
            else:
                self.__balls[ball_nr].hide()


    def getCircleCoords(self, radius, x_offset=0.0, z_offset=0.0, phase=0.0, period=2.0, num_points=100):
        radians = ((math.pi * period) / num_points)
        radians_offset = (math.pi * phase)
        return [x_offset - math.cos(radians * x + radians_offset) * radius for x in xrange(0, num_points + 1)], \
               [z_offset - math.sin(radians * z + radians_offset) * radius for z in xrange(0, num_points + 1)]


    def drawField(self):
        self.field = vis.box(pos=(0, -0.05, 0),
                             size=(self.field_dimensions[0] * 2, 0.1, self.field_dimensions[1] * 2),
                             color=Colours.green)

        self.score_board = vis.label(pos=(0, 10, -1.5*self.field_dimensions[1]), text=str(""))

        self.left_goal = Goal(pos=(-self.field_dimensions[0], 0, 0))
        self.right_goal = Goal(pos=(self.field_dimensions[0], 0, 0))
        self.middle_line = vis.curve(x=[0, 0],
                                     z=[-self.field_dimensions[1], self.field_dimensions[1]],
                                     color=Colours.white)
        self.field_edge = vis.curve(x=[-self.field_dimensions[0], +self.field_dimensions[0],
                                       +self.field_dimensions[0], -self.field_dimensions[0],
                                       -self.field_dimensions[0],],
                                    z=[-self.field_dimensions[1], -self.field_dimensions[1],
                                       +self.field_dimensions[1], +self.field_dimensions[1],
                                       -self.field_dimensions[1],],
                                    color=Colours.white)

        self.left_23 = vis.curve(x=[-self.field_dimensions[0] + 23, -self.field_dimensions[0] + 23],
                                 z=[-self.field_dimensions[1], self.field_dimensions[1]])

        self.right_23 = vis.curve(x=[self.field_dimensions[0] - 23, +self.field_dimensions[0] - 23],
                                  z=[-self.field_dimensions[1], self.field_dimensions[1]])


    def drawSemiCircle(self, radius, left, dotted=False):
        if left:
            x_offset = -self.field_dimensions[0]
            x_rad = +radius
            period = 0.5
            phase_1 = 0.5
            phase_2 = 1.0
        else:
            x_offset = self.field_dimensions[0]
            x_rad = -radius
            period = 0.5
            phase_1 = 0
            phase_2 = 1.5

        semi_circle = []
        semi_circle.append(self.__drawCircle(radius=radius,
                                             x_offset=x_offset,
                                             z_offset=-self.left_goal.dimensions[2] / 2,
                                             period=period,
                                             phase=phase_1,
                                             dotted=dotted))

        semi_circle.append(vis.curve(x=[x_offset + x_rad,
                                        x_offset + x_rad],
                                     z=[-self.left_goal.dimensions[2] / (6 if dotted else 2),
                                        self.left_goal.dimensions[2] / (6 if dotted else 2)]))

        semi_circle.append(self.__drawCircle(radius=radius,
                                             x_offset=x_offset,
                                             z_offset=self.left_goal.dimensions[2] / 2,
                                             period=period,
                                             phase=phase_2,
                                             dotted=dotted))
        return semi_circle


    def drawCircles(self):
        # self.circle = self.__drawCircle(radius=5)
        self.left_circle = self.drawSemiCircle(radius=14.63, left=True)
        self.right_circle = self.drawSemiCircle(radius=14.63, left=False)

        self.left_circle_dotted = self.drawSemiCircle(radius=19.63, left=True, dotted=True)

        self.right_circle_dotted = self.drawSemiCircle(radius=19.63, left=False, dotted=True)


    def __drawCircle(self, **kwargs):
        if "dotted" in kwargs:
            dotted = kwargs["dotted"]
            del kwargs["dotted"]
        else:
            dotted = False
        kwargs["num_points"] = 53 * 5
        coords = self.getCircleCoords(**kwargs)
        if dotted:
            dotted_circle = []
            for part in range(0, 53 + 1, 2):
                dotted_circle.append(vis.curve(x=coords[0][part * 5:part * 5 + 5],
                                               z=coords[1][part * 5:part * 5 + 5],
                                               color=Colours.white))
            return dotted_circle
        else:
            return vis.curve(x=coords[0], z=coords[1], color=Colours.white)


    def drawDots(self):
        self.left_dot = self.__drawDot((-self.field_dimensions[0] + 5, 0))

        self.middle_dot = self.__drawDot((0, 0))

        self.right_dot = self.__drawDot((self.field_dimensions[0] - 5, 0))


    def __drawDot(self, pos):
        return vis.cylinder(pos=(pos[0], -0.09, pos[1]),
                                 axis=(0, 0.1, 0),
                                 radius=0.2,
                                 color=Colours.white)


    def __del__(self):
        if self.input_file is not None:
            self.input_file.close()

