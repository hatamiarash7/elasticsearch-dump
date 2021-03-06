#!/usr/bin/env python
# encoding: utf-8

import linecache
import logging
import sys
import threading
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from utils import count_file_lines

try:
    import simplejson as json
except ImportError:
    import json

"""
Instructions:
    There are 3 processes of importing raw JSON data to ElasticSearch
    1. Only validating raw JSON data
    2. Without validating ,just import data to ElasticSearch
    3. After validating successfully, then import data to ElasticSearch
"""

es = Elasticsearch(['http://localhost:9200'], verify_certs=False)

total_lines = 0


def show_help():
    print("""
Options include:

--data                  : The data file
--check                 : Validate data file
--bulk                  : ElasticSearch endpoint ( http://localhost:9200 )
--index                 : Index name
--type                  : Index type
--import                : Import data to ES
--thread                : Threads amount, default = 1
--help                  : Display this help 
""")


def validate_json_data(json_file=""):
    """
    Validate the JSON data
    """
    if str(json_file) == "":
        raise ValueError("No JSON file was input\n")
    else:
        try:
            f = open(json_file, encoding="utf8")
        except IOError as e:
            raise IOError('Can not open the file "%s" , error : \n%s\n' % (json_file, str(e)))
        else:
            f.close()

            with open(json_file, encoding="utf8") as f:
                for index, line in enumerate(f):
                    print("Validate", index + 1, "Of", total_lines, end="\r")
                    # convert each line into Python object
                    try:
                        _ = json.loads(line)
                    except Exception as e:
                        print("JSON data not valid with error \n %s \n" % (str(e)))
                        return False
                    else:
                        pass
            return True


def import_for_threading(data='test_data.json', start_line=0, stop_line=0, elastic=es, index="", doc_type=""):
    actions = []
    try_times = 0
    # Use linecache to put data in RAM
    for i in range(int(start_line), int(stop_line) + 1):
        row = linecache.getline(data, i)
        try:
            action = {
                "_index": index,
                "_type": doc_type,
                "_source": json.loads(row)
            }
        except Exception as e:
            logging.warning(str(e))
            continue

        actions.append(action)

        if len(actions) >= 5000:
            # try to connect ES
            while try_times < 5:
                try:
                    helpers.bulk(elastic, actions)
                    try_times = 0
                    break
                except Exception as e:
                    try_times = try_times + 1
                    logging.warning("Can not send a group of docs to ElasticSearch using parallel_bulk, error : " + str(e))
                    time.sleep(5)
            if try_times >= 5:
                msg = "After trying " + str(try_times) + \
                      " times. It still can not send a group of docs to ElasticSearch using parallel_bulk"
                logging.error(msg)
                try_times = 0

            del actions[0:len(actions)]

    # clear all the caches out of the loop every time
    linecache.clearcache()

    if len(actions) > 0:
        try:
            helpers.bulk(elastic, actions)
        except Exception as e:
            logging.warning("Can not send a group of docs to ElasticSearch using parallel_bulk, with error: " + str(e))
        del actions[0:len(actions)]

    return


def calculate_lines(lines=0, thread_amount=1):
    """
    Return lines to read for each thread equally

    for example. 37 lines, 4 threads

    [
        {"start": 1, "stop": 10},
        {"start": 11, "stop": 20},
        {"start": 21, "stop": 30},
        {"start": 31, "stop": 37}
    ]

    start from line 1, ends at line 37 (includes line 37 in data file)

    lets assume if there were 17 lines and 4 threads,
    thread (1)(2)(3) can have 5 job tasks maximally. thread (4) only has 2 job tasks

    their job list:
                  iter 0        iter 1             iter 2            iter 3
                  thread 1      thread 2           thread 3          thread 4
    line/job num  1,2,3,4,5     6,7,8,9,10         11,12,13,14,15    16,17

    iter means iteration
    """

    line_list = []
    each_has = lines / thread_amount
    last_remains = lines % thread_amount

    for t in range(thread_amount):
        line_list.append(
            {
                "start": each_has * t + 1,
                "stop": each_has * (t + 1)
            }
        )

    if last_remains > 0:
        line_list[-1] = {
            "start": each_has * (thread_amount - 1) + 1,
            "stop": lines
        }

    return line_list


def run():
    global total_lines

    start_time = time.time()

    if len(sys.argv) == 1:
        show_help()
        return
    else:
        process_jobs = []

        for i in range(len(sys.argv[0:])):
            if sys.argv[i].startswith("--"):
                try:
                    option = sys.argv[i][2:]
                except Exception as e:
                    logging.warning("You forgot something ! Error : " + str(e))
                    show_help()
                    return

                if option == "help":
                    show_help()
                    return
                elif option == "data":
                    process_jobs.append({"data": sys.argv[i + 1]})
                elif option == "bulk":
                    process_jobs.append({"bulk": sys.argv[i + 1]})
                elif option == "index":
                    process_jobs.append({"index": sys.argv[i + 1]})
                elif option == "type":
                    process_jobs.append({"type": sys.argv[i + 1]})
                elif option == "check":
                    process_jobs.append("check")
                elif option == "import":
                    process_jobs.append("import")
                elif option == "thread":
                    process_jobs.append({"thread_amount": sys.argv[i + 1]})
                    process_jobs.append("thread")

        data = ""
        bulk = ""
        index = ""
        doc_type = ""
        thread_amount = 1

        # Get info from process_jobs
        for job in process_jobs:
            if type(job) == dict:
                if 'data' in job:
                    data = job['data']
                if 'bulk' in job:
                    bulk = job['bulk']
                if 'index' in job:
                    index = job['index']
                if 'type' in job:
                    doc_type = job['type']
                if 'thread' in job:
                    thread_amount = int(job['thread_amount'])

        total_lines = count_file_lines(data)
        print("Total JSON data lines : " + str(total_lines))

        # 1 : no import / check
        if ("check" in process_jobs) and ("import" not in process_jobs):
            print("Run : Only check")

            if validate_json_data(json_file=data):
                print("All raw JSON data valid!")
            return

        # 2.1 : import / check / single-thread
        if ("check" in process_jobs) and ("import" in process_jobs) and ("thread" not in process_jobs):
            print("Run : Import using single-thread with validation")

            if validate_json_data(json_file=data):
                print("All raw JSON data valid!")

            es1 = Elasticsearch([bulk], verify_certs=True)
            with open(data, encoding="utf8") as f:
                for i, line in enumerate(f):
                    print("Import", i + 1, "Of", total_lines, end="\r")
                    es1.index(index=index, doc_type=doc_type, body=json.loads(line))

            print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
            return

        # 2.2 : import / no check / single-thread
        if ("check" not in process_jobs) and ("import" in process_jobs) and ("thread" not in process_jobs):
            print("Run : Import using single-thread without validation")

            es2 = Elasticsearch([bulk], verify_certs=True)
            with open(data, encoding="utf8") as f:
                for i, line in enumerate(f):
                    print("Import", i + 1, "Of", total_lines, end="\r")
                    es2.index(index=index, doc_type=doc_type, body=json.loads(line))

            print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
            return

        # 2.3 : import / no check / multi-threads
        if ("import" in process_jobs) and ("check" not in process_jobs) and ("thread" in process_jobs):
            if total_lines < 1024:
                print("This file is not big enough to use multi-threading !")
                print("Run : Import using single-thread without validation")

                es3 = Elasticsearch([bulk], verify_certs=True)
                with open(data, encoding="utf8") as f:
                    for line in f:
                        es3.index(index=index, doc_type=doc_type, body=json.loads(line))
            else:
                print("Run : Import using multi-thread without validation")

                # calculate how many lines should be read for each thread
                line_list = calculate_lines(lines=total_lines, thread_amount=thread_amount)
                threads = []

                for line in line_list:
                    t = threading.Thread(target=import_for_threading,
                                         args=(
                                             data,
                                             line['start'],
                                             line['stop'],
                                             Elasticsearch([bulk], verify_certs=True),
                                             index,
                                             doc_type
                                         )
                                         )
                    threads.append(t)
                    t.start()
                    t.join()

                # stop all threads if interrupts
                try:
                    while len(threading.enumerate()) > 1:
                        pass
                    print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
                    return
                except KeyboardInterrupt:
                    print("Data importing interrupted!")
                    exit(0)
                    return

            print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
            return

        # 2.4 : import / check / multi-threads
        if ("import" in process_jobs) and ("check" in process_jobs) and ("thread" in process_jobs):
            if validate_json_data(json_file=data):
                print("All raw JSON data valid!")
            if total_lines < 1024:
                print("This file is not big enough to use multi-threading !")
                print("Run : Import using multi-thread with validation")

                es4 = Elasticsearch([bulk], verify_certs=True)
                with open(data, encoding="utf8") as f:
                    for line in f:
                        es4.index(index=index, doc_type=doc_type, body=json.loads(line))
                print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
                exit(0)
                return
            else:
                print("Run : Import using multi-thread with validation")

                # calculate how many lines should be read for each thread
                line_list = calculate_lines(lines=total_lines, thread_amount=thread_amount)
                threads = []

                for line in line_list:
                    t = threading.Thread(target=import_for_threading,
                                         args=(
                                             data,
                                             line['start'],
                                             line['stop'],
                                             Elasticsearch([bulk], verify_certs=True),
                                             index,
                                             doc_type
                                         )
                                         )
                    threads.append(t)
                    t.start()
                    t.join()

                # stop all threads if interrupts
                try:
                    while len(threading.enumerate()) > 1:
                        pass
                    print("Your data imported successfully in : ", time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
                    exit(0)
                    return
                except KeyboardInterrupt:
                    print(len(threading.enumerate()))
                    print("Data importing interrupted!")
                    exit(0)
                    return
        else:
            show_help()
            return


if __name__ == "__main__":
    run()
    exit(0)
