# -*- coding: utf-8 -*-
import os
import argparse
import timeloop
from datetime import timedelta
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('interval', nargs='?', default=30, type=int, help='Send data interval in sec')
parser.add_argument('database', nargs='?', default='nalog', type=str, help='Influx database name')
parser.add_argument('cols', nargs='*', type=str,
                    default='time,key,duration,operation,message,status,istatus,report'.split(","),
                    help='Comma-separated influx fields')
args = parser.parse_args()

t1 = timeloop.Timeloop()

zabbix_server = "n7701-sys951"
zabbix_host = "n7701-sys420"
home = os.getcwd()
if not os.path.exists(f"{home}/dat"):
    os.makedirs(f"{home}/dat")
sender_exe = "D:/zabbix_agent/bin/win64/zabbix_sender.exe"
influx_exe = "D:/influx/influxdb/influx.exe"
text_cols = ",".join([f'"{x}"' for x in args.cols])
query = f'select {text_cols} from autotests ' \
        f'where time>now()-{args.interval}s order by time desc'
query = query.replace("\"", "\\\"")


@t1.job(interval=timedelta(seconds=args.interval))
def execute():
    data = []
    res = []

    influx_p = subprocess.Popen(f"{influx_exe} -database {args.database} -execute \"{query}\" -format csv -precision s",
                                stdout=subprocess.PIPE)

    lines = influx_p.stdout.readlines()
    influx_p.stdout.close()
    t1.logger.info(f"exported: {len(lines)};")
    for n, line in enumerate(lines):
        if n > 0:
            data.append([x.replace("\n", "") for x in line.decode("utf-8").split(",")])

    for line in data:
        res.append("{} {} {} \"{}\"".format(zabbix_host, args.database, line[1], ",".join(line).replace("\"", "'")))
        res.append("{} {} {} \"{}\"".format(zabbix_host, line[2], line[1], ",".join(line).replace("\"", "'")))

    with open(f"{home}/dat/{args.database}.dat", "w", encoding='utf8') as f:
        f.truncate()
        f.write("\n".join(res))
        f.close()

    zbx_p = subprocess.Popen(f"{sender_exe} -z {zabbix_server} -s {zabbix_host} -T -i {home}/dat/{args.database}.dat",
                             stdout=subprocess.PIPE)
    lines = zbx_p.stdout.readlines()
    t1.logger.info(lines[0].decode('utf-8'))


if __name__ == '__main__':
    execute()
    t1.start(True)
