from queue_logic import sorted_queue, _in_shift


MANAGERS = {
    'a': {'name': 'Alice', 'max_leads': None},
    'b': {'name': 'Bob',   'max_leads': None},
    'c': {'name': 'Carl',  'max_leads': 2},
}


def test_sorted_queue_orders_by_taken_count():
    queue = sorted_queue(
        managers=MANAGERS,
        taken_map={'a': 3, 'b': 1, 'c': 0},
        avail_map={'a': True, 'b': True, 'c': True},
        overrides={},
        sent_map={},
    )
    assert queue == ['c', 'b', 'a']


def test_sorted_queue_excludes_unavailable_and_pending():
    queue = sorted_queue(
        managers=MANAGERS,
        taken_map={'a': 0, 'b': 0, 'c': 0},
        avail_map={'a': True, 'b': False, 'c': True},
        overrides={},
        sent_map={'c': 1},
    )
    # b — недоступний, c — вже очікує відповіді (pending), лишається тільки a
    assert queue == ['a']


def test_sorted_queue_respects_max_leads_override():
    queue = sorted_queue(
        managers=MANAGERS,
        taken_map={'a': 0, 'b': 0, 'c': 2},
        avail_map={'a': True, 'b': True, 'c': True},
        overrides={'a': 0},
        sent_map={},
    )
    # a має ручний override=0 (ліміт вичерпано), c досяг власного max_leads=2
    assert queue == ['b']


def test_in_shift_normal_hours():
    # Пн-Пт (0-4), 16:00-23:00
    days = [0, 1, 2, 3, 4]
    assert _in_shift('18:00', weekday=2, days=days, start='16:00', end='23:00') is True
    assert _in_shift('15:59', weekday=2, days=days, start='16:00', end='23:00') is False
    assert _in_shift('23:00', weekday=2, days=days, start='16:00', end='23:00') is False
    assert _in_shift('18:00', weekday=5, days=days, start='16:00', end='23:00') is False


def test_in_shift_crosses_midnight():
    # Зміна 22:00-05:00, працює в понеділок (0)
    days = [0]
    assert _in_shift('23:30', weekday=0, days=days, start='22:00', end='05:00') is True
    assert _in_shift('04:00', weekday=1, days=days, start='22:00', end='05:00') is True
    assert _in_shift('06:00', weekday=1, days=days, start='22:00', end='05:00') is False
    assert _in_shift('21:00', weekday=0, days=days, start='22:00', end='05:00') is False
