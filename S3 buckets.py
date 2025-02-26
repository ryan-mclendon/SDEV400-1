'''
program that allows S3 bucket creation and manipulation

Charles McLendon
SDEV 400 7380
31 Oct 2023
Professor Craig Poma
'''

import random
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

def main ():
    '''
    runs the menu for user to select the action they wish to take
    loops continuously until user decides to exit by inputting 7
    also begins logging to document any errors

    :param s3(S3.Client) passsed to all functions to access S3 resources
    '''

    s3 = boto3.client('s3')
    logging.basicConfig(filename='./error.log',
                        format='%(levelname)s: %(asctime)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filemode='a',
                        level=logging.INFO)

    print("Welcome to the S3 bucket menu.")

    while True:

        print("========S3 Bucket Options========\n"
            "1. Create a new S3 bucket\n"
            "2. Put an object in a bucket\n"
            "3. Delete an object from a bucket\n"
            "4. Delete a bucket\n"
            "5. Copy an object from one bucket to another\n"
            "6. Download an object from a bucket\n"
            "7. Exit the program\n"
            "=================================\n")
        choice = input("Please enter the number for the action you would like to perform: ")
        match choice:
            case "1":
                create_bucket(s3)
            case "2":
                place_object(s3)
            case "3":
                delete_object(s3)
            case "4":
                delete_bucket(s3)
            case "5":
                copy_object(s3)
            case "6":
                download_object(s3)
            case "7":
                print("Thank you for using the program. Goodbye")
                print(datetime.now().isoformat(timespec='seconds'))
                break
            case _:
                print("Invalid input; please pick a choice 1 - 7\n")

def create_bucket(s3):
    '''
    function to create a bucket by combining user defined input and a 6-digit num

    param: first(str): user's first name
    param: last(str): user's last name
    param: bucket_name(str): projected name for bucket based on first, last, and random number
    '''
    first = ""
    last = ""

    while not check_valid_name(first, last):
        first = input("Enter your first name: ").lower()
        last = input("Enter your last name: ").lower()

    bucket_name = first + last + create_random()

    while True:

        if check_bucket_exists(s3, bucket_name):
            print(bucket_name)
            bucket_name = first + last + create_random()
            continue

        s3.create_bucket(Bucket=bucket_name)
        print(f"Your bucket {bucket_name} was successfully created!")
        break

def check_valid_name(first, last):
    '''
    Checks to ensure name for bucket is DNS compliant
    
    param: first(str): user's first name
    param: last(str): user's last name
    param: name(str): combined string of first and last
    rtype: **bool**
    return: True if name is DNS compliant, otherwise False + specific reason why
    '''

    name = first + last

    if len(name) == 0:
        return False

    if first == "xn--" or first == "sthree-" or first[0] == "-":
        print("Invalid first name, please try again")
        return False

    name = name.replace("-", "")

    if not name.isalpha():
        print("Name contains invalid characters, please try again")
        return False

    if len(name) > 58:
        print("Name is too long, please try again")
        return False

    return True

def create_random():
    '''
    creates a 6-digit number to append to the end of a bucket name

    param: random_num(str) fucntions uses this to build the number
    param: i(int) used to create for loop
    rtype: **str**
    return: a 6-digit string of numbers
    '''
    random_num = ""

    for i in range(6):
        random_num += str(random.randrange(0,10))

    return random_num

def check_bucket_exists(s3, bucket_name):
    '''
    checks to see if a bucket name is already in use
    does a get request via the head_bucket function

    param: bucket_name(str) potential bucket name to be created
    rtype: **bool**
    return: True if bucket exists, False if does not exist
    '''

    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as c:
        logging.info(c)
        return False
    return True

def place_object(s3):
    '''
    takes a file and puts it into a bucket of the user's choice

    uses display_buckets() to create a list for the user to choose from

    param: index(int) corresponds to user selected bucket
    param: bucket_choice(str) name of bucket chosen by user
    param: bucket_len(int) used to escape function if no buckets are found
    param: e(Exception) logs and display error if file not found
    '''
    while True:

        bucket_len = display_buckets(s3)

        if bucket_len == 0:
            print("Please create a bucket before attempting to place an object")
            return

        index = input("Please enter the number that corresponds to the "
                        "bucket you would like to place your object into: ")

        bucket_choice = get_bucket(s3, index)

        if bucket_choice == -1:
            print("Invalid choice, please try again")
            continue
        print(f"You selected: {bucket_choice}")
        break

    try:
        s3.upload_file("error.log",bucket_choice,"error.log")

        print("File successfully uploaded!")

    except Exception as e:
        print(e)
        logging.error(e)

def display_buckets(s3):
    '''
    Lists all buckets available to the user
    used by multiple functions to easily display all bucket options
    
    param: response(dict) dictionary of S3's bucket properties
    param: bucket_list(list) list to hold all the bucket names
    param: i(int) used to create a table-like display for the user
    param: bucket(str) individual bucket names
    rtype: **int**
    return: length of list to check if no buckets exist
    '''

    response = s3.list_buckets()
    bucket_list = [bucket['Name'] for bucket in response['Buckets']]

    print("Bucket list: ")
    i = 1
    for bucket in bucket_list:

        print(str(i) + ". " + bucket)
        i += 1

    return len(bucket_list)

def get_bucket(s3, index):
    '''
    retrieve bucket name based on user defined index
    allows user to select bucket without having to type full name
    used after a user selects a bucket to pull the individual name
    and also ensure user input a correct option

    param: response(dict) dictionary of S3's bucket properties
    param: bucket_list(list) list to hold all the bucket names
    param: index(int) user selection from the list of buckets
    rtype: **str**
    return: Bucket name chosen by user, or -1 if user input bad choice
    '''
    response = s3.list_buckets()
    bucket_list = [bucket['Name'] for bucket in response['Buckets']]

    if not index.isdigit() or int(index) > len(bucket_list) or int(index) < 1:
        return -1

    return bucket_list[int(index) - 1]

def delete_object(s3):
    '''
    allows a user to delete an object
    user selects the bucket they want and then the object inside that bucket
    user also has to confirm deletion by typing yes

    param: index(int) corresponds to user selected bucket
    param: bucket_len(int) used to escape function if no buckets are found
    param: bucket_choice(str) name of bucket chosen by user
    param: object_choice(str) name of object to be deleted
    param: confirmation(str) user inputs yes or no to confirm deletion
    param: e(Exception) logs and display error if file not found
    '''

    while True:

        bucket_len = display_buckets(s3)

        if bucket_len == 0:
            print("Please create a bucket before attempting to delete an object")
            return

        index = input("Please enter the number that corresponds to the "
                        "bucket you would like to delete from: ")

        bucket_choice = get_bucket(s3, index)

        if bucket_choice == -1:
            print("Invalid choice, please try again")
            continue

        print(f"You selected: {bucket_choice}")
        break

    while True:

        object_len = display_objects(s3,bucket_choice)

        if object_len == 0:
            return

        index = input("Please enter the number that corresponds to the "
                        "object you would like to delete: ")

        object_choice = get_object(s3, bucket_choice, index)

        if object_choice == -1:
            print("Invalid choice, please try again")
            continue

        confirmation = input(f"Are you sure you want to delete {object_choice}? yes or no: ")

        if confirmation.lower() == "yes":
            try:
                s3.delete_object(Bucket=bucket_choice, Key=object_choice)
                print(f"{object_choice} successfully deleted.")
            except ClientError as e:
                logging.error(e)
                print(e)

        else:
            print("Delete object aborted")
        break

def display_objects(s3, bucket):
    '''
    Lists all buckets available to the user
    similar to display_buckets() funtion, used by other functions to
    display all objects in a particular bucket in a table-like format

    param: bucket(str) user chosen bucket, validity checked before this is called
    param: response(dict) dictionary of S3's bucket properties
    param: object_list(list) list to hold all the object names
    param: index(int) user selection from the list of buckets
    rtype: **int**
    return: length of list to check if no objects exist
    '''

    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' not in response.keys():
        print("No objects in selected bucket")
        return 0

    object_list = [objects['Key'] for objects in response['Contents']]

    print("Object list: ")
    i = 1
    for objects in object_list:

        print(str(i) + ". " + objects)
        i += 1

    return len(object_list)

def get_object(s3, bucket, index):
    '''
    gets object chosen by user
    similar to get_bucket() function, used by other functions

    param: bucket(str) user chosen bucket, validity checked before this is called
    param: index(int) user selection from the list of objects
    param: object_list(list) all objects in the selected bucket
    param: choice(str) object name that corresponds to user selection
    param: folder_check(str) display_objects() also shows folder names, 
    this is used to verify user did not select a folder instead of a file
    rtype: **str**
    return: name of object, -1 if user selected a folder
    '''
    response = s3.list_objects_v2(Bucket=bucket)
    object_list = [objects['Key'] for objects in response['Contents']]

    if not index.isdigit() or int(index) > len(object_list) or int(index) < 1:
        return -1

    choice = object_list[int(index) - 1]

    if any("/" for char in choice):
        index = choice.rfind("/")+1
        folder_check = choice[index:]
        if folder_check == "":
            return -1

    return choice

def delete_bucket(s3):
    '''
    displays a list of buckets and allows a user to delete one if empty
    
    param: index(int) corresponds to user selected bucket
    param: bucket_len(int) used to escape function if no buckets are found
    param: bucket_choice(str) name of bucket chosen by user
    param: confirmation(str) user inputs yes or no to confirm deletion
    '''

    while True:
        bucket_len = display_buckets(s3)

        if bucket_len == 0:
            print("Please create a bucket before attempting to delete a bucket")
            return

        index = input("Please enter the number that corresponds to the "
                        "bucket you would like to delete: ")

        bucket_choice = get_bucket(s3, index)

        if bucket_choice == -1:
            print("Invalid choice, please try again")
            continue

        confirmation = input(f"Are you sure you want to delete {bucket_choice}? yes or no: ")

        if confirmation.lower() == "yes":
            try:
                s3.delete_bucket(Bucket=bucket_choice)
                print(f"{bucket_choice} successfully deleted")
            except ClientError as e:
                logging.error(e)
                print(e)
        else:
            print("Bucket delete aborted")
        break

def copy_object(s3):
    '''
    copies an object from one bucket to another
    user chooses origin bucket and object then destination bucket
    origin and destination bucket cannot be the same

    param: index(int) corresponds to user selected bucket
    param: bucket_len(int) used to escape function if no buckets are found
    param: origin_bucket(str) name of first bucket chosen by user
    param: destination_bucket(str) name of 2nd bucket chosen by user
    param: copy_source(dict) used to build metadata for object for copying
    '''

    while True:
        bucket_len = display_buckets(s3)

        if bucket_len == 0:
            print("Please create a bucket before attempting to copy an object")
            return

        index = input("Please enter the number that corresponds to the "
                        "bucket you would copy from: ")

        origin_bucket = get_bucket(s3, index)

        if origin_bucket == -1:
            print("Invalid choice, please try again")
            continue

        print(f"You selected: {origin_bucket}")
        break

    while True:

        object_len = display_objects(s3,origin_bucket)

        if object_len == 0:
            return

        index = input("Please enter the number that corresponds to the "
                        "object you would like to copy: ")

        object_choice = get_object(s3, origin_bucket, index)

        if object_choice == -1:
            print("Invalid choice, please try again")
            continue
        break

    copy_source = {'Bucket': origin_bucket, 'Key': object_choice}

    while True:

        display_buckets(s3)

        index = input("Please enter the number that corresponds to the "
                        "bucket you would copy to: ")

        destination_bucket = get_bucket(s3, index)

        if destination_bucket in (-1, origin_bucket):
            print("Invalid choice, please try again")
            continue

        print(f"You selected: {destination_bucket}")

        try:
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=object_choice)
            print("Object successfully copied")
        except ClientError as e:
            logging.error(e)
            print(e)
        break

def download_object(s3):
    '''
    downloads the selected object to local machine
    in this case the local machine is the cloud9 environment
    user selects bucket then file they wish to download

    param: index(int) corresponds to user selected bucket
    param: bucket_len(int) used to escape function if no buckets are found
    param: bucket_choice(str) name of bucket chosen by user
    param: object_choice(str) name of object to be deleted
    param: file_name(str) name of file after it downloads
    '''
    while True:

        bucket_len = display_buckets(s3)

        if bucket_len == 0:
            print("Please create a bucket before attempting to download an object")
            return

        index = input("Please enter the number that corresponds with the "
                        "bucket you would like to download from: ")

        bucket_choice = get_bucket(s3, index)

        if bucket_choice == -1:
            print("Invalid choice, please try again")
            continue

        print(f"You selected: {bucket_choice}")
        break

    while True:

        object_len = display_objects(s3,bucket_choice)

        if object_len == 0:
            return

        index = input("Please enter the number that corresponds to the "
                        "object you would like to download: ")

        object_choice = get_object(s3, bucket_choice, index)

        if object_choice == -1:
            print("Invalid choice, please try again")
            continue

        file_name = object_choice

        if any("/" for char in object_choice):
            index = object_choice.rfind("/")+1
            file_name = object_choice[index:]

        try:
            s3.download_file(bucket_choice, object_choice,
                             f'/home/ec2-user/environment/{file_name}')
            print(f"{object_choice} successfully downloaded.")
        except ClientError as e:
            logging.error(e)
            print(e)

        break

main()
