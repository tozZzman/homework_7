import re
import json
import argparse
import os
from collections import defaultdict
from datetime import datetime
import time

parser = argparse.ArgumentParser(description='Simple parser of server logs')
parser.add_argument('--logfile', type=str, help='entering the path to the log file', default=None)
parser.add_argument('--logdir', type=str, help='entering the path to the directory with logs', default=None)
# parser.add_argument('--savepath', type=str, help='entering the path to the directory with the results', default='.')
parser.add_argument('--limit', type=int, help='entering the limit of parsed lines', default=100)
args = parser.parse_args()

logfile = args.logfile
logdir = args.logdir
savepath = 'results'
limit = args.limit

# if __name__ == '__main__':
#     logdir = 'dir'
#     logfile = None
#     savepath = 'results'
#     limit = 1000


def logparse(log, outlimit):
    try:
        dict_ip = defaultdict(
            lambda: {"GET": 0, "POST": 0, "PUT": 0, "DELETE": 0, "HEAD": 0}
        )
        with open(file=log, mode='r') as f:
            idx = 0
            for line in f:
                if idx > outlimit:
                    break
                ip = re.search(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}', line)
                if ip is not None:
                    method = re.search(r'] "(POST|GET|PUT|DELETE|HEAD)', line).groups()[0]
                    dict_ip[ip.group()][method] += 1
                idx += 1

        top = top_ip(**dict_ip)
        dict_ip['top_ip'] = top
        long_ip = top_long_requests(log, outlimit)
        dict_ip['top_long_requests'] = long_ip

        return dict_ip
    except FileNotFoundError:
        print('Такого файла не существует')


def search_logs(path):
    try:
        logs = []
        for i in os.listdir(path):
            filename, file_extension = os.path.splitext(i)
            if file_extension == '.log':
                logs.append(i)
        logs = [os.path.join(path, log) for log in logs]
        return logs
    except FileNotFoundError:
        print('Такой директории не существует')


def top_ip(**log):
    dict_ip = defaultdict(
        lambda: {'ip': None, 'requests': None, 'methods': None}
    )

    items = list(log.items())
    weight_dict = dict()

    for item in items:
        weight = sum(list(item)[1].values())
        weight_dict[list(item)[0]] = weight
    list_w = list(weight_dict.items())
    list_w.sort(key=lambda i: i[1], reverse=True)

    idx = 1

    for item in list_w[:3]:
        dict_ip[idx]['ip'] = item[0]
        dict_ip[idx]['requests'] = item[1]
        dict_ip[idx]['methods'] = log[item[0]]
        idx += 1

    return dict(dict_ip)


def top_long_requests(log, outlimit):
    top_dict_ip = defaultdict(
        lambda: {"ip": None, "method": None, "url": None, "time": None}
    )

    with open(file=log, mode='r') as f:
        idx = 0
        time_req = 'start'

        dict_ip = defaultdict(
            lambda: {"ip": None, "method": None, "url": None, "time": None}
        )

        for line in f:
            if idx > outlimit:
                break
            ip = re.search(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}', line)

            if ip is not None:

                dict_ip[idx]['ip'] = ip.group()
                method = re.search(r'] "(POST|GET|PUT|DELETE|HEAD)', line).groups()[0]
                dict_ip[idx]['method'] = method
                url = re.search(r'"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                                line)
                if url is not None:
                    dict_ip[idx]['url'] = url.group()

                pattern = r'\d{,3}/\w{,3}/\d{,4}:\d{,2}:\d{,2}:\d{,2}'
                format_date = '%d/%b/%Y:%H:%M:%S'

                if time_req == 'start':
                    time_req = 0
                    dict_ip[idx]['time'] = time_req

                    stime = re.search(pattern, line)
                    start_time = datetime.strptime(stime.group(), format_date)

                time_req = re.search(pattern, line).group()
                end_time = datetime.strptime(time_req, format_date)
                delta = end_time - start_time
                dict_ip[idx]['time'] = delta.total_seconds()
                start_time = end_time

            idx += 1

        list_ip = list(dict_ip.items())
        list_ip.sort(key=lambda i: i[1]['time'], reverse=True)

        ids = 1

        for item in list_ip[:3]:
            top_dict_ip[ids] = item[1]
            ids += 1

    return dict(top_dict_ip)


def save_json(log, filename, save_path):
    if '\\' in filename:
        sepr = filename.rfind('\\')
        filename = filename[sepr + 1:]
    name = os.path.join(save_path,
                        f"parse_{filename}_{str(datetime.now())[:19].replace(' ', '_').replace(':', '-')}.json")

    with open(file=name, mode='w') as f:
        f.write(json.dumps(log, indent=4, sort_keys=True))

    return name


def show_stout(log):
    with open(log, 'r') as f:
        print(f'========================> Parsed log output: {log}')
        for line in f.readlines():
            print(line.replace('\n', ''))


if logfile is not None and logdir is None:
    res = logparse(logfile, limit)
    out = save_json(res, logfile[:-4], savepath)
    show_stout(out)

elif logdir is not None and logfile is None:
    files = search_logs(logdir)
    for file in files:
        res = logparse(file, limit)
        out = save_json(res, file[:-4], savepath)
        time.sleep(1)
        show_stout(out)

elif logdir is not None and logfile is not None:
    print('Не допускается одновременная передача аргументов logdir и logfile')

else:
    files = search_logs(os.getcwd())
    for file in files:
        res = logparse(file, limit)
        out = save_json(res, file[:-4], savepath)
        time.sleep(1)
        show_stout(out)
