
import apache_beam as beam


class MergeResult(beam.CombineFn):
    # 統合後のPCollectionを作成
    def create_accumulator(self):
        print("create")
        return ([], [])

    # 各要素に対して実行
    def add_input(self, accumulator, element):
        print("add", "acc", accumulator, "ele", element)
        sets, names = accumulator
        return sets.append(element), names.append(element.get("name"))

    # すべてのaccumulatorを統合
    def merge_accumulators(self, accumulators):
        print("merge:", accumulators)
        return accumulators

    # 最終処理
    def extract_output(self, accumulator):
        print("extract:", accumulator)
        return accumulator
