from pymavlink import mavutil
import csv
import time, datetime
import sys, os
import threading
import cv2
import numpy as np
import queue
from dataclasses import dataclass
import argparse

@dataclass
class FrameData:
    csv_row: list
    image: np.ndarray

@dataclass
class MavData:
    csv_row: list

def write_from_buffer(t0, vio_cap_dir):
    global write_buffer

    mav_fname = vio_cap_dir+"/mav_imu.csv"
    stamp_fname = vio_cap_dir+"/cam.csv"
    video_fname = vio_cap_dir+"/cam.mkv"

    video_writer = None
    with open(stamp_fname, 'w') as stamp_file, open(mav_fname, 'w') as mav_file:
        
        # Set up video data writer
        stamp_writer = csv.writer(stamp_file)
        stamp_writer.writerow([
            "Timestamp (s)",
            "Frame #",
            "Exposure"
        ])

        # Set up IMU data writer
        mav_writer = csv.writer(mav_file)
        mav_writer.writerow([
            "Timestamp (s)",
            "xgyro (rad/s)",
            "ygyro (rad/s)",
            "zgyro (rad/s)",
            "xacc (m/s/s)",
            "yacc (m/s/s)",
            "zacc (m/s/s)",
            "Mav Time (s)",
            "Roll (rad)",
            "Pitch (rad)",
            "Yaw (rad)",
            "Lat (deg)",
            "Lon (deg)",
            "Alt (m)",
            "GLat (deg)",
            "GLon (deg)",
            "GAlt (m)",
        ])

        print("Writing buffer is started.")

        flush_counter = 0
        max_flush_count = 200
        while True:
            data_item = write_buffer.get(block=True)
             
            if isinstance(data_item, MavData):
                mav_writer.writerow(data_item.csv_row)
            elif isinstance(data_item, FrameData):
                if video_writer is None:
                    # Initialise the video writer
                    frame = data_item.image
                    video_writer = cv2.VideoWriter(video_fname, cv2.VideoWriter_fourcc(*args.codec), 20.0, (frame.shape[1],frame.shape[0]))
                video_writer.write(data_item.image)
                stamp_writer.writerow(data_item.csv_row)
            
            flush_counter += 1
            if flush_counter % max_flush_count == 0:
                mav_file.flush()
                stamp_file.flush()









def record_cam(t0, indoor_lighting = True): 
    global write_buffer

    cap = cv2.VideoCapture(0)
    cap.set(cv2.CAP_PROP_AUTO_EXPOSURE, 0.25) #means manual


    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720) 
    if indoor_lighting:
        exposure = 0.62 # Indoor initial value
    else:
        exposure = 0.001 # Outdoor initial value
    gain = 1e-4
    
    ret, frame = cap.read()

    frame_count = 0
    print("Video recording is started.")
    while(True):
        ret, frame = cap.read()
        if not ret:
            print("Video frame did not return correctly!")
            continue
        t = time.time()
        row = [ t - t0, frame_count, exposure ]

        write_buffer.put(FrameData(row, frame))
        
        frame_count += 1

        # Control camera exposure
        cap.set(cv2.CAP_PROP_EXPOSURE, exposure)
        img_mean = np.mean(frame)

        if img_mean > 128-32 and img_mean < 128+32:
            continue

        exposure += gain * (128 - img_mean) * exposure
        if exposure > 0.7:
            exposure = 0.7
        elif exposure <= 0.0:
            exposure = 1e-6

        time.sleep(0.08)
            


def record_imu(t0):
    global write_buffer

    gyro_factor = 1e-3
    acc_factor = 9.81 * 1e-3

    ATTITUDE = None
    GLOBAL_POSITION_INT = None
    GPS2_RAW = None

    print("MAV/IMU recording is started.")
    while(True):
        msg = mav_connection.recv_match(type=["RAW_IMU",'ATTITUDE','GLOBAL_POSITION_INT','GPS2_RAW'], blocking=True)
        mtype = msg.get_type()
        if mtype == 'ATTITUDE':
            ATTITUDE = msg
            continue
        if mtype == 'GLOBAL_POSITION_INT':
            GLOBAL_POSITION_INT = msg
            continue
        if mtype == 'GPS2_RAW':
            GPS2_RAW = msg
            continue
        if mtype != 'RAW_IMU':
            continue
        if ATTITUDE is None or GLOBAL_POSITION_INT is None or GPS2_RAW is None:
            continue
        t = time.time()
        assert msg is not None
        row = [
            t - t0,
            msg.xgyro * gyro_factor,
            msg.ygyro * gyro_factor,
            msg.zgyro * gyro_factor,
            msg.xacc * acc_factor,
            msg.yacc * acc_factor,
            msg.zacc * acc_factor,
            msg.time_usec*1.0e-6,
            ATTITUDE.roll,
            ATTITUDE.pitch,
            ATTITUDE.yaw,
            GLOBAL_POSITION_INT.lat*1.0e-7,
            GLOBAL_POSITION_INT.lon*1.0e-7,
            GLOBAL_POSITION_INT.alt*0.001,
            GPS2_RAW.lat*1.0e-7,
            GPS2_RAW.lon*1.0e-7,
            GPS2_RAW.alt*0.001,
        ]
    
        write_buffer.put(MavData(row))


def request_message_interval(master : mavutil.mavudp, message_id: int, frequency_hz: float):
    """
    Request MAVLink message in a desired frequency,
    documentation for SET_MESSAGE_INTERVAL:
        https://mavlink.io/en/messages/common.html#MAV_CMD_SET_MESSAGE_INTERVAL

    Args:
        message_id (int): MAVLink message ID
        frequency_hz (float): Desired frequency in Hz
    """
    master.mav.command_long_send(
        master.target_system, master.target_component,
        mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, 0,
        message_id, # The MAVLink message ID
        1e6 / frequency_hz, # The interval between two messages in microseconds. Set to -1 to disable and 0 to request default rate.
        0, # Target address of message stream (if message has target address fields). 0: Flight-stack default (recommended), 1: address of requestor, 2: broadcast.
        0, 0, 0, 0)


parser = argparse.ArgumentParser("Record camera, IMU, GPS, attitude.")
parser.add_argument("-i", action='store_true', help="Use initial exposure estimate for indoor lighting.")
parser.add_argument("--no_video", action='store_true', help="Disable video recording.")
parser.add_argument("--codec", type=str, default="H264", help="The desired video writer codec. Default: 'H264'")
args = parser.parse_args()



print("Requesting Connection to MAV...")
# mav_connection = mavutil.mavlink_connection('udpout:raspberrypi.local:14550', )

mav_connection = mavutil.mavlink_connection('/dev/serial0', baud=921600)
# mavproxy.py --aircraft Qubit --master /dev/serial0 --baudrate 921600 --out=udpin:0.0.0.0:14550

# mav_connection.mav.ping_send(222,0,0,0)
mav_connection.wait_heartbeat()
print("Heartbeat from system (system %u component %u)" % (mav_connection.target_system, mav_connection.target_system))

print("Requesting 200 Hz IMU")

# RAW_IMU at 200Hz
request_message_interval(mav_connection, mavutil.mavlink.MAVLINK_MSG_ID_RAW_IMU, 200.0)

# ATTITUDE and GLOBAL_POSITION_INT at 20Hz
request_message_interval(mav_connection, mavutil.mavlink.MAVLINK_MSG_ID_GLOBAL_POSITION_INT, 20.0)
request_message_interval(mav_connection, mavutil.mavlink.MAVLINK_MSG_ID_GPS2_RAW, 20.0)
request_message_interval(mav_connection, mavutil.mavlink.MAVLINK_MSG_ID_ATTITUDE, 20.0)


now = datetime.datetime.now()
dt = now.strftime("%Y-%m-%d %H-%M-%S")
vio_cap_dir = "VIO Capture {}".format(dt)
os.mkdir(vio_cap_dir)

# Start the IMU and camera threads
write_buffer = queue.Queue()
t0 = time.time()

imu_thread = threading.Thread(target=record_imu, args=(t0,))
imu_thread.start()
if args.no_video:
    print("Video recording is not active!")
else:
    cam_thread = threading.Thread(target=record_cam, args=(t0, args.i))
    cam_thread.start()
write_thread = threading.Thread(target=write_from_buffer, args=(t0, vio_cap_dir))
write_thread.start()

print("Data is saved to {}/".format(vio_cap_dir))

