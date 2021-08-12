import boto3
from datetime import datetime
import json
import pytz
import requests

URL = 'https://www.followmychallenge.com/live/aebr21/api/gbduro/'
PATH = 'racingCollective/duro/gb/flow.json'


def update_race_flow():
    flow_data = requests.get(URL).json()
    converted_flow_data = RaceFlowConverter(datetime(2021, 8, 7, 8, tzinfo=pytz.UTC)).convert_flow_data(flow_data)
    to_s3(PATH, converted_flow_data)


class RaceFlowConverter(object):

    def __init__(self, start_time):
        self.target_kmph = 12.0
        self.start_time = start_time

    def convert_flow_data(self, rider_data):
        flow_data = {}
        color = color_generator()
        for grp, riders in rider_data.items():
            for riderName, riderTrack in riders.items():
                initials = ''.join(n[0] for n in riderName.split(' '))
                flow_data[initials] = {'color': next(color),
                                       'data': self.convert_track(riderTrack)}
        flow_data_json = json.dumps(flow_data)
        return flow_data_json

    def convert_track(self, track):
        converted_track = []
        last_ts = -300
        last_d = -1
        for ts, d in track.items():
            ts = int(ts)
            d = float(d)
            if (ts - last_ts) >= 300 and 0 < (d - last_d) < 200:
                elapsed = pytz.utc.localize(datetime.fromtimestamp(ts)) - self.start_time
                elapsed_hours = elapsed.days * 24 + elapsed.seconds / 60 / 60
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
            '#17becf'   # blue-teal
        ]:
            yield color


def to_s3(path, body):
    s3 = boto3.resource('s3')
    s3.Object('bikerid.es', path).put(Body=body, ContentType='application/json')
