
import apache_beam as beam


class MergeResult(beam.CombineFn):
    """
    PCollectionに対し､
    output1: すべてのPCollectionをListに格納したもの
    output2: PCollectionのMapのkey(=keyword)を持つものを抽出する｡
    """

    def __init__(self, keyword):
        """
        keywordのKeyのリストを作成
        """
        self.keyword = keyword

    def create_accumulator(self):
        """
        accumulatorを定義
        """
        # print("create")
        return ([], [])

    def add_input(self, accumulator, element):
        """
        各PCollectionをaccumulatorに格納
        """
        # print("add", "acc", accumulator, "ele", element)
        sets, names = accumulator
        sets.append(element)
        names.append(element.get(self.keyword))
        return sets, names

    # すべてのaccumulatorを統合
    def merge_accumulators(self, accumulators):
        """
        accumulatorを結合
        """
        return accumulators

    def extract_output(self, accumulator):
        """
        最終処理
        """
        sets, names = accumulator[0][0], accumulator[0][1]
        return {"sets": sets, "names": names}
