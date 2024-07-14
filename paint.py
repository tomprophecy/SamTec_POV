#!/usr/bin/env python

import rospy
from turtlesim.msg import Pose
from turtlesim.srv import SetPen
from math import floor

def pose_callback(pose):
    x=pose.x
    y=pose.y
    dis = ((x-5.544444561)**2 + (y-5.544444561)**2)**0.5
    alpha = (5**0.5 - 1)/2
    color = round(255*(1.0/(alpha + dis/7.841028694) - alpha))
    color = abs(color)
    d=color
    pen(d,d,d,20,0)

if __name__ == '__main__':
    # The pen handle (set below) will be used in 'pose_callback' so we make it
    # global.  Another way of handling this would be to wrap this functionality
    # into a class.
    global pen

    # Initialize this node.
    rospy.init_node('init')
    rospy.wait_for_service('turtle1/set_pen')

    # Create a handle for calling the set_pen service (e.g. tutorial 15)

    pen = rospy.ServiceProxy('turtle1/set_pen', SetPen)
    rospy.Subscriber("turtle1/pose",Pose,pose_callback)
    pen(255,255,255,20,0)
    # This prevents the node from exiting, which is necessary since we want to
    # continue receiving callbacks when the turtle moves.
    rospy.spin()
