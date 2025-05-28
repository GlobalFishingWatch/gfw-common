from gfw.common import decorators


def test_timing():
    def work(result):
        return result

    expected = "test1"
    res, elapsed = decorators.timing(work)(result=expected)
    assert res == expected
    assert isinstance(elapsed, float)


def test_timing_quiet():
    def work(result):
        return result

    expected = "test1"
    res, elapsed = decorators.timing(work, quiet=True)(result=expected)
    assert res == expected
    assert isinstance(elapsed, float)


def test_counter():
    size = 3
    assert len(list(decorators.counter((x for x in range(size))))) == size
