from app.preprocessing.pipeline import (
    preprocess_text,
    preprocess_sim,
    preprocess_timeseries,
)


def test_preprocess_text_calls_adapter_for_each_row(mocker):
    rows = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    fake_docs = [object(), object(), object()]

    to_textdoc = mocker.patch(
        "app.preprocessing.pipeline.to_textdoc",
        side_effect=fake_docs,
    )

    out = preprocess_text(rows)

    assert out == fake_docs
    assert to_textdoc.call_count == len(rows)
    for call_arg, row in zip(to_textdoc.call_args_list, rows):
        assert call_arg.args[0] == row


def test_preprocess_text_empty_returns_empty_list(mocker):
    to_textdoc = mocker.patch(
        "app.preprocessing.pipeline.to_textdoc", return_value=object()
    )
    out = preprocess_text([])
    assert out == []
    to_textdoc.assert_not_called()


def test_preprocess_sim_calls_adapter_for_each_item(mocker):
    items = [{"m": "a"}, {"m": "b"}]
    fake_docs = [object(), object()]

    to_simdoc = mocker.patch(
        "app.preprocessing.pipeline.to_simdoc",
        side_effect=fake_docs,
    )

    out = preprocess_sim(items)
    assert out == fake_docs
    assert to_simdoc.call_count == len(items)
    for call_arg, item in zip(to_simdoc.call_args_list, items):
        assert call_arg.args[0] == item


def test_preprocess_sim_empty_returns_empty_list(mocker):
    to_simdoc = mocker.patch(
        "app.preprocessing.pipeline.to_simdoc", return_value=object()
    )
    out = preprocess_sim([])
    assert out == []
    to_simdoc.assert_not_called()


def test_preprocess_timeseries_calls_adapter_for_each_item(mocker):
    items = [{"path": "a.csv"}, {"path": "b.csv"}]
    fake_docs = [object(), object()]

    to_tsdoc = mocker.patch(
        "app.preprocessing.pipeline.to_tsdoc",
        side_effect=fake_docs,
    )

    out = preprocess_timeseries(items)
    assert out == fake_docs
    assert to_tsdoc.call_count == len(items)
    for call_arg, item in zip(to_tsdoc.call_args_list, items):
        assert call_arg.args[0] == item


def test_preprocess_timeseries_empty_returns_empty_list(mocker):
    to_tsdoc = mocker.patch(
        "app.preprocessing.pipeline.to_tsdoc", return_value=object()
    )
    out = preprocess_timeseries([])
    assert out == []
    to_tsdoc.assert_not_called()
