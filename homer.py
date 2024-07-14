#!/usr/bin/env python  
 
import rospy
# Add other import statements if necessary
 
# Import the formats needed for used services and messages
from turtlesim.msg import Color
from turtlesim.srv import TeleportRelative
from geometry_msgs.msg import Twist

prev = 0
def sensor_callback(color):
	global prev
	r=color.r
	intensity = r
	if intensity == prev:
		turtle1_teleport(0.01,0)
	if intensity > prev:
		vel_msg.linear.y = 2
		vel_msg.linear.z = 0
		vel_msg.angular.x = 0
		vel_msg.angular.y = 0
		vel_msg.angular.z = 0
		vel_publisher.publish(vel_msg)
	if intensity < prev:
		vel_msg.linear.y = 2
		vel_msg.linear.z = 0
		vel_msg.angular.x = 0
		vel_msg.angular.y = 0
		vel_msg.angular.z = 2
		vel_publisher.publish(vel_msg)
	prev=intensity


if __name__ == '__main__':
	global vel_publisher # ??? ANY OTHER VARIABLES NEED TO BE GLOBAL?
	global vel_msg
	global turtle1_teleport
	rospy.init_node('homer')
	# Create a publisher so that we can output command velocities to the turtle.
	vel_publisher = rospy.Publisher("turtle1/cmd_vel",Twist,queue_size=10)
	vel_msg=Twist()
	turtle1_teleport = rospy.ServiceProxy('turtle1/teleport_relative', TeleportRelative)
	# Subscribe to the color sensor.
	rospy.Subscriber("turtle1/color_sensor",Color,sensor_callback)
	rospy.spin()