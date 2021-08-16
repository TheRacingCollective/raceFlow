import boto3
from datetime import datetime
import json
import pytz
import requests

URL = 'https://www.followmychallenge.com/live/gbduro21/api/gbduro/'
PATH = 'racingCollective/duro/gb/21FT.json'
FEMALE = ['Philippa Battye', 'Jaimi Wilson', 'Sharn Hooper', 'Alice Lemkes', 'Lee Craigie', 'Emily Harper',
          'Naomi Freireich', 'Claire Stevens', 'Beth Jackson', 'Rose Osborne', 'Jill Dawes', 'Victoria Peel',
          'Clare Walkeden', 'Izzy Freshwater']
MALE = ['Angus Young', 'Mark Beaumont', 'Ed Milbourn', 'Ollie Hayward', 'Lee Brown', 'Adam Colvin', 'Carl Hopps',
        'Isaac Hudson', 'Nick Spencer-Vellacott', 'Jim Higgins', 'Leslie Brown', 'Simon Wareing', 'Charlie Holden',
        'Chris Simpson', 'Howard Perkins', 'Liam Yates', 'George Bramwell', 'Ben Rickaby', 'Lewis Clark',
        'Stephen Fryer', 'Anthony Carton']


def lambda_wrapper(event, lambda_context):
    update_race_flow()


def update_race_flow(write_local=False):
    flow_data = requests.get(URL).json()
    start_time = datetime(2021, 8, 14, 7, tzinfo=pytz.utc)
    converted_flow_data = RaceFlowConverter(start_time, 8.8, end_distance=632).convert_flow_data(flow_data)
    to_s3(PATH, converted_flow_data)
    if write_local:
        with open('flow.json', 'w') as flow_file:
            flow_file.write(converted_flow_data)


class RaceFlowConverter(object):

    def __init__(self, start_time, target_kmph=12., distance_offset=0, end_distance=None):
        self.target_kmph = target_kmph
        self.start_time = start_time
        self.end_distance = end_distance
        self.distance_offset = distance_offset

    def convert_flow_data(self, rider_data):
        flow_data = []
        color = color_generator()
        if rider_data:
            for grp, riders in rider_data.items():
                if grp == 3:
                    continue
                for riderName, riderTrack in riders.items():
                    initials = self.get_initials(riderName)
                    groups = self.get_groups(riderName, grp)
                    flow_data.append({'name': riderName,
                                      'initials': initials,
                                      'color': next(color),
                                      'groups': groups,
                                      'data': self.convert_track(riderTrack)})
        ordered_flow_data = sorted(flow_data, key=lambda x: max(y['x'] for y in x['data']), reverse=True)
        flow_data_json = json.dumps(ordered_flow_data)
        return flow_data_json

    @staticmethod
    def get_initials(rider_name):
        return ''.join(n[0] for n in rider_name.split(' '))

    @staticmethod
    def get_groups(rider_name,  group):
        if rider_name in MALE:
            return 'M'
        if rider_name in FEMALE:
            return 'F'
        if group == '2':
            return 'P'

    def convert_track(self, track):
        converted_track = []
        start_ts = self.start_time.timestamp()
        last_ts = -300
        last_d = 0
        for ts, d in track.items():
            ts = int(ts)
            d = float(d)
            if ts >= start_ts and (ts - last_ts) >= 150 and d >= last_d and (self.end_distance is None or d <= self.end_distance):
                elapsed = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc) - self.start_time
                elapsed_hours = elapsed.days * 24 + elapsed.seconds / 60 / 60
                d = d - self.distance_offset
                converted_track.append({
                    'x': d,
                    'y': round(elapsed_hours - d / self.target_kmph, 2)})
                last_ts = ts
                last_d = d
        return converted_track


def color_generator():
    while True:
        for color in [
            '#1f77b4',  # muted blue
            '#ff7f0e',  # safety orange
            '#2ca02c',  # cooked asparagus green
            '#d62728',  # brick red
            '#9467bd',  # muted purple
            '#8c564b',  # chestnut brown
            '#e377c2',  # raspberry yogurt pink
            '#7f7f7f',  # middle gray
            '#bcbd22',  # curry yellow-green
            '#17becf'  # blue-teal
        ]:
            yield color


def to_s3(path, body):
    s3 = boto3.resource('s3')
    s3.Object('bikerid.es', path).put(Body=body, ContentType='application/json')
