#!/usr/bin/env python

import caproto
from caproto.threading.client import Context
import lxml
import lxml.etree
import untangle
import attr
import typing

import os
import time
import enum
import queue


class CaprotoStore():
    def __init__(self, folder):
        self.f = folder
    def write(self, pv_name, response):
        with open(os.path.join(self.f, pv_name) + ".ca", "ab") as f:
            f.write(bytes(response))

cs = CaprotoStore("data")

class AccessType(enum.IntEnum):
    Monitor = 1
    Scan = 2

@attr.s
class ConfigChannel():
    pv_name = attr.ib()
    group = attr.ib(type=str, default='')
    period = attr.ib(type=float, default=0.0)
    access = attr.ib(type=int, default=AccessType.Monitor)

@attr.s
class Config():
    channels = attr.ib(type=typing.List[ConfigChannel], default=attr.Factory(list))

def get_config_xml_untangle(xml_config_file):
    obj = untangle.parse(xml_config_file)
    for group in obj.engineconfig.group:
        print(group.name['tag'])
    return Config()

def get_config_xml_lxml(xml_config_file):
    with open(xml_config_file, 'rb') as f:
        tree = lxml.etree.parse(f)
    channels = []
    for group in tree.findall('group'):
        group_name = group.find('name').text
        for channel in group.findall('./channel'):
            access = AccessType.Monitor if channel.find('monitor') is not None else AccessType.Scan
            cc = ConfigChannel(
                channel.find('name').text,
                group=group_name,
                access=access,
                )
            channels.append(cc)
    return Config(channels=channels)

def connection_changed(pv, state):
    if state == 'connected':
        #print("state-change:connected", pv.name)
        log = caproto.SubscriptionType.DBE_LOG
        prop = caproto.SubscriptionType.DBE_PROPERTY
        sub = pv.subscribe(mask=log | prop)
        #: fix currently needed for https://github.com/caproto/caproto/issues/607
        #sub.clear()
        sub.add_callback(update)
    else:
        pass # print(state)

responses = queue.Queue()

def update(sub, response):
    global responses
    print("value-update", sub.pv.name, response.data[0])
    #import pdb; pdb.set_trace()
    responses.put((sub.pv.name, response))

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--xml-config-file')
    args = parser.parse_args()
    if not args.xml_config_file:
        parser.error('please provide one type of configuration file')
    if args.xml_config_file:
        #config = get_config_xml_untangle(args.xml_config_file)
        config = get_config_xml_lxml(args.xml_config_file)

    ctx = Context(timeout=None)
    pv_names = (cc.pv_name for cc in config.channels)
    pv_names = set(pv_names)
    pvs = ctx.get_pvs(*pv_names,
                      connection_state_callback=connection_changed)
    print(" ".join(pv_names))
    try:
        while True:
            time.sleep(1)
            print("total responses received:", responses.qsize())
            try:
                while responses.qsize():
                    pv_name, response = responses.get(block=False)
                    # add to archived data...
                    #print(pv_name, response.data[0])
                    cs.write(pv_name, response)
            except queue.Empty:
                pass
    except KeyboardInterrupt:
        print()

if __name__ == "__main__":
    main()
