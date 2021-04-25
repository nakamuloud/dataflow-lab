import apache_beam as beam
from google.protobuf.timestamp_pb2 import Timestamp
import ast


class AddNameProperty(beam.DoFn):
    """
    add lastname and firstname key-value based on name key
    """

    def __init__(self, delimiter=' '):
        self.delimiter = delimiter

    def process(self, input):
        data = dict(ast.literal_eval(input.decode("unicode_escape")))
        data["firstname"] = data.get("name").split(" ")[0]
        data["lastname"] = data.get("name").split(" ")[-1]
        yield data


class Compute(beam.DoFn):
    """
    do next step
    """

    def __init__(self):
        pass

    def process(self, input):
        print(input)
        yield input
