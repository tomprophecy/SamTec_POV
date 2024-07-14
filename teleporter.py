#!/usr/bin/env python

import rospy
# Add other import statements if necessary

# Import the formats needed for used services and messages
from turtlesim.srv import TeleportRelative

def spiral():
    # After teleporting the robot should take a sleep.  If we go too
    # quickly then the teleports happen faster than calls to pose_callback.
    # This means that the pen's colour is not appropriately updated before each
    # teleport (try increasing the rate and you'll see what happens).
    rate = rospy.Rate(2)
    s=1
    turtle1_teleport(0,1.57079632679)
    for i in range(27):
		for j in range(s):
			turtle1_teleport(0.44,0)
			rate.sleep()
		turtle1_teleport(0,1.57079632679)
		for j in range(s):
			turtle1_teleport(0.44,0)
			rate.sleep()
		turtle1_teleport(0,1.57079632679)
		s=s+1


if __name__ == '__main__':

	rospy.init_node('teleporter')
	rospy.wait_for_service('turtle1/teleport_relative')
	turtle1_teleport = rospy.ServiceProxy('turtle1/teleport_relative', TeleportRelative)
	spiral()

    # Sometimes we need to let let a node stay active so that it can
    # respond to callbacks.  We can do this with:
    #   rospy.spin()
    # However, all of the work is now done so we can just let the node die.
