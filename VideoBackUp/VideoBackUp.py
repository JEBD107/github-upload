import cv2
import numpy as np
import boto3
import os
import datetime
import pytz
import sys


class S3:
    def __init__(self, aws_key="aws_key",
                 aws_secret="aws_secret"):
        try:
            # S3 connection
            print("Creation of S3 object")
            self.s3 = boto3.resource("s3", aws_access_key_id=aws_key,
                                     aws_secret_access_key=aws_secret)
        except Exception as e:
            print("Some wrong when init S3 object, error {}".format(e))
            exit(-1)

    def bucket(self, bucket="defaultbucket"):
        try:
            print("Selection of bucket")
            self.bucket = self.s3.Bucket(bucket)
        except Exception as e:
            print("Some wrong when select bucket, error {}".format(e))
            exit(-1)

    def load_bucket(self, file, key, erase=False):
        try:
            print("Load into bucket")
            self.bucket.upload_file(file, key)
        except Exception as e:
            print("Can't load, error: {}".format(e))
        else:
            print("File was uploaded")
            if erase:
                try:
                    os.remove(file)
                    print("File was deleted")
                except Exception as e:
                    print("Some wrong when erase file, error {}".format(e))
            else:
                print("File was not removed")

    def download(self, prefix, destiny, limit=0):
        print("Download Method")
        i = 0
        print("Loop")
        for bucket_object in self.bucket.objects.filter(Prefix=prefix):
            file = bucket_object.key
            print("Object key: " + file)
            file_name = file.split("/")[-1]
            print("File name: " + file_name)
            path = file.replace(file_name, "")
            path = os.path.join(destiny, path)
            print("Path: " + path)
            try:
                if os.path.isdir(path):
                    print("The path exist")
                else:
                    print("Make the path")
                    os.makedirs(path)
            except Exception as e:
                print("Error trying to make path: {}".format(e))
            else:
                try:
                    self.bucket.download_file(file, os.path.join(path, file_name))
                except Exception as e:
                    print("Error trying to download the object: {} error: {}".format(file, e))
                else:
                    print("Delete object: " + file)
                    bucket_object.delete()
                    i = i + 1
            if limit == 0:
                print("Nothing to do")
            elif i >= limit:
                break


class Video:
    def __init__(self, url, file, format):
        try:
            # Stream connection
            print("Stream connection")
            self.capture = cv2.VideoCapture(url)
            fourcc = cv2.VideoWriter_fourcc(*"XVID")
            self.video = cv2.VideoWriter(file + ".avi", fourcc, format[0], (format[1], format[2]))
            self.frame = format[0]
        except Exception as e:
            print("Some wrong when init Stream object, error {}".format(e))
            exit(-1)

    def record(self, time):
        try:
            print("Start recording")
            for i in range(0, int(self.frame * 60 * time)):
                flag, frame = self.capture.read()
                if flag==True:
                    self.video.write(frame)
                else:
                    print("Some error in frame capture")
                    break
        except Exception as e:
            print("Some wrong when record, error {}".format(e))
            exit(-1)

    def __del__(self):
        self.capture.release()
        self.video.release()


if __name__ == "__main__":
    print("Main program")
    now = datetime.datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    if now.hour < 10 or now.hour > 22:
        print("It it out of range")
    else:
        date = "{:02d}-{:02d}-{}".format(now.day, now.month, now.year)
        time = "{:02d}-{:02d}-{:02d}".format(now.hour, now.minute, now.second)
        s3 = S3()
        s3.bucket("newbucket")
        video1 = Video(sys.argv[1], time, (float(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])))
        video1.record(int(sys.argv[3]))
        del video1
        s3.load_bucket(time + ".avi", sys.argv[2] + date + "/" + time + ".avi", True)
