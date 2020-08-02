#!/usr/bin/env python

from beautyacc import Archive
from btsf import MetricType, Metric, BinaryTimeSeriesFile
from btsf import EmptyBtsfError
from btsf import IntroSection, IntroSectionHeader, IntroSectionType
import jsons as json
import attr

import struct
import re
# from multiprocessing.dummy import Pool
from multiprocessing import Pool
import os
from datetime import datetime as dt, timedelta


@attr.s
class Task:
    pv_name = attr.ib()
    output_folder = attr.ib()
    db_host = attr.ib()
    db_port = attr.ib()
    db_user = attr.ib()
    db_name = attr.ib()


def process_task(task):
    if (
        any(char in task.pv_name for char in ("/", "(", ")"))
        or (task.pv_name == "jane")
        or (task.pv_name == "freddy")
    ):
        print(task)
        return {"pv_name": task.pv_name, "success": False}
    a = Archive(task.db_host, user=task.db_user, port=task.db_port, dbname=task.db_name)
    parts = task.pv_name.split(":")
    target_folder = os.path.join(task.output_folder, "/".join(parts[:-1]))
    os.makedirs(target_folder, exist_ok=True)
    target_filename = os.path.join(target_folder, parts[-1] + ".btsf")
    # os.system('touch ' + target_filename)
    channel_id = a.channelid_of_pvname(task.pv_name)
    channel = a.fetch_channel(channel_id)
    group = a.fetch_chan_grp(channel.grp_id)
    target_column = a.infer_target_column(task.pv_name)
    if target_column == 'float_val':
        m_type = MetricType.Double
    elif target_column == 'num_val':
        m_type = MetricType.Int64
    elif target_column == 'str_val':
        a.close()
        return {"pv_name": task.pv_name, "success": False}
    else:
        raise NotImplementedError(target_column)

    n_samples = 0

    metrics = [
        Metric("time", MetricType.Double, is_time=True),
        Metric(task.pv_name, m_type),
    ]

    try:
        with BinaryTimeSeriesFile.openread(target_filename) as btsf:
            ts, value = btsf.last()
        start = dt.fromtimestamp(ts + 0.001)

        def btsf_factory():
            return BinaryTimeSeriesFile.openwrite(target_filename)

    except (FileNotFoundError, EmptyBtsfError):
        start = None

        def btsf_factory():
            annotations = {}
            payload = b"{}"
            annotation_intro = IntroSection(
                header=IntroSectionHeader(
                    type=IntroSectionType.Annotations,
                    payload_size=len(payload),
                    followup_size=512 * 1024 + (-len(payload) % 16),
                ),
                payload=payload,
            )
            return BinaryTimeSeriesFile.create(
                target_filename, metrics, intro_sections=[annotation_intro]
            )

    with btsf_factory() as btsf:

        nan = float("nan")
        i = 0
        for ts, value in a.iter_single_pv(
            task.pv_name, target=target_column, start=start
        ):
            n_samples += 1
            ts = ts.timestamp()
            if value is None:
                if target_column == "num_val":
                    value = 0
                elif target_column == "float_val":
                    value = nan
            try:
                btsf.append(ts, value)
            except struct.error as e:
                print(
                    i,
                    task.pv_name,
                    a.channelid_of_pvname(task.pv_name),
                    target_column,
                    ts,
                    value,
                )
                raise e
            i += 1
    a.close()

    return {
        "pv_name": task.pv_name,
        "success": True,
        "channel": channel,
        "group": group,
        "n_samples": n_samples,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("db_host")
    parser.add_argument("--db-user", default="report")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="archive")
    parser.add_argument("--processes", type=int, default=8)
    parser.add_argument("--include", action="append", help="PVs to be included (RegEx - default: .*)", default=[r".*"])
    parser.add_argument("--exclude", action="append", help="PVs to be excluded (RegEx - none by default)", default=[])
    parser.add_argument("output_folder")
    args = parser.parse_args()
    print(args)

    a = Archive(args.db_host, user=args.db_user, port=args.db_port, dbname=args.db_name)
    all_pv_names = a.all_pv_names
    all_pv_names = [
        p for p in all_pv_names
        if any(re.match(pattern, p) for pattern in args.include)
        and not any(re.match(pattern, p) for pattern in args.exclude)
    ]
    all_pv_names = [p for p in all_pv_names if a.infer_target_column(p) != "str_val"]
    a.close()

    print(len(all_pv_names))
    tasks = [
        Task(
            pv_name=pv,
            output_folder=args.output_folder,
            db_host=args.db_host,
            db_port=args.db_port,
            db_user=args.db_user,
            db_name=args.db_name,
        )
        for pv in all_pv_names
    ]
    with Pool(processes=args.processes) as pool:
        failed = []
        done = []
        total = len(tasks)
        for result in pool.imap_unordered(process_task, tasks):
            if result["success"] == False:
                print(f"✘ {result['pv_name']}")
                failed.append(result)
            else:
                print(
                    f"✔ {result['pv_name']} ({len(done)+1}/{total}{f', ✘: {len(failed)}' if failed else ''})"
                )
                done.append(result)
        print(f"Successfully processed {len(done)} PVs")


if __name__ == "__main__":
    main()
