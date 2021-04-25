"""shadow data pipeline processing"""
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from datetime import datetime
from package.input import input
import random
import base64
import json
import gzip


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--debug', type=str)


"""Main entry point; defines and runs the wordcount pipeline."""
# ParDo 内で実行される関数は別マシンでの実行のため、ライブラリのimportがされていない扱いになる。
# save_main_sessinon で引き継ぐようになる

pipeline_options = PipelineOptions()
user_options = pipeline_options.view_as(UserOptions)
pipeline_options.view_as(StandardOptions).streaming = True
pipeline_options.view_as(SetupOptions).save_main_session = True


#############################
# PIPELINE DEFINITIONS
#############################

def run():
    p = beam.Pipeline(options=pipeline_options)
    messages = (p
                | beam.io.ReadFromPubSub(with_attributes=True,
                                         subscription="projects/mobilitygw-develop/subscriptions/develop-sp-subscription")
                | "GetLastKnownVehicleShadow" >> GroupMessagesByFixedWindows(5.0, 1)
                | "Decode" >> beam.Map(input.decode_and_decompress)
                | beam.Map(print)
                )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
