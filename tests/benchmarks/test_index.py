from dvc_data.index import DataIndexEntry


def test_entry(benchmark):
    def _create_entry():
        DataIndexEntry()

    benchmark.pedantic(_create_entry, rounds=100000)
