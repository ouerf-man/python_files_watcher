from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import boto3
import botocore
import os
import time
import sys
from datetime import datetime

s3 = boto3.client("s3")

#This block is used to exit the code if no args are passed to the script

try:
    if (not sys.argv[1] and not sys.argv[2]):
        exit()
except Exception:
    print("Please provide bucketname as first argument")
    exit()
    
# Here we store the bucket name and path passed in the arguments 

bucket = sys.argv[1]
path = sys.argv[2]

""" 
    This global variable (last_trigger) is used in the on_modified handler because watchdog trigger on_modfied event twice so it's a workaround 
    I found in this discussion https://github.com/gorakhargosh/watchdog/issues/346
 """
 
last_trigger = time.time()

""" 
    This function is used to handle the exceptions while listening for event files, 
    we only pass so the code doesn't leave the execution.
 """
def handle_exception():
    # code here
    pass

#This function is user to test if a file exists on s3 or not
    
def file_exists(filename):
    try:
        boto3.resource("s3").Bucket(bucket).Object(filename).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist.
        return True

""" 
    This class implements the FileSystemEventHandler exposed by watchdog, 
    we implement file creation handler and modified handler here
"""
class FileHandler(FileSystemEventHandler):

    def on_created(self, event):
        if(os.path.isdir(event.src_path)):
            return
        
        head, tail = os.path.split(event.src_path)
        name, ext = os.path.splitext(tail)
        
        if(ext=='.swp' or os.path.isdir(event.src_path)):
            return
        print("Created: " + event.src_path)
        s3.upload_file(
            Filename=event.src_path,
            Bucket=bucket,
            Key=tail,
        )

    def on_modified(self, event):
        global last_trigger
        current_time = time.time()
        if event.src_path.find('~') == -1 and (current_time -
                                               last_trigger) > 1:
            last_trigger = current_time
            head, tail = os.path.split(event.src_path)
            name, ext = os.path.splitext(tail)
            if(ext=='.swp' or os.path.isdir(event.src_path)):
                return
            print("Modified: " + event.src_path)
            s3.upload_file(
                Filename=event.src_path,
                Bucket=bucket,
                Key=name + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + ext,
            )

    #code execution start here
if __name__ == "__main__":
    
    """ Here we walk the files inside the directory and 
    test if they exist on s3 if not upload them """
    
    print("Sync files...")
    for (dir_path, dir_names, file_names) in os.walk(os.path.abspath(path)):
        for file_name in file_names:
            try:
                if (not file_exists(file_name)):
                    print(file_name, ' uploading ...')
                    s3.upload_file(
                        Filename=os.path.join(dir_path, file_name),
                        Bucket=bucket,
                        Key=file_name,
                    )
                else:
                    print(file_name, ' already exists.')
            except botocore.exceptions.ClientError as e:
                print(e)

    print('Files sync done.')
    
    try:
        """ Here we create an instance of the class we implemented above 
        and pass it to the observer in the schedule method """
        event_handler = FileHandler()

        # Create an observer.
        observer = Observer()

        # Attach the observer to the event handler.
        observer.schedule(event_handler, os.path.abspath(path), recursive=True)

        # Start the observer.
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
    except Exception:
        handle_exception()