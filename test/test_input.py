import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import package.process as process
import package.output as output
from unittest import TestCase
import json
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import ast


def load_data(path, type="dict"):
    """
    [{}]形式のjsonを読み込んで､list[dict|bytes]を返す
    """
    with open(path) as f:
        if(type == "dict"):
            data = ast.literal_eval(f.read())
        elif(type == "bytes"):
            data = ast.literal_eval(f.read())
            data = map(lambda x: json.dumps(x).encode("unicode_escape"), data)
        else:
            exit(1)
        return data


class TestExtractNameProperty(TestCase):
    def test_transform(self):
        """
        入力データに姓名を追加
        """
        inputs = load_data("./data/sample.json", type="bytes")

        with TestPipeline() as p:
            step1 = (p
                     | "Init" >> beam.Create(inputs)
                     | "ExtractNameProperty" >> beam.ParDo(process.AddNameProperty(" "))
                     )

            step2 = (step1
                     # 合算時以外にも呼ばれる,returnのみ１回呼ばれる
                     | "CombineResult" >> beam.CombineGlobally(output.MergeResult())
                     | "Print" >> beam.Map(print)
                     )
            assert_that(step1, equal_to(
                load_data("./data/sample.output.json", type="dict")), label="並列で姓名を追加できている")
            # assert_that(step2, load_data(
            #     "./data/sample.output.json", type="dict"), label="並列処理の結果を正しく結合できている")
