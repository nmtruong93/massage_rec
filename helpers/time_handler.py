from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta


def convert_int_to_date(date_int, input_format="%Y%m%d", output_format="%Y-%m-%d"):
    """
    Convert int to date

    :param date_int: int, date in int. Ex: 20190101
    :param input_format: str, input format
    :param output_format: str, output format
    :return: str, date in str
    """
    return datetime.strptime(str(date_int), input_format).strftime(output_format)


def get_list_date_range(start_date, end_date, output_format="%Y%m%d", include_end_date=False):
    """
    Get list of date range, [start_date, end_date). Exclusive end_date

    :param start_date: str, start date. Ex: 20190101 or 2019-01-01
    :param end_date: str, end date. Ex: 20190101 or 2019-01-01
    :param output_format: str, date format. Ex: %Y%m%d or %Y-%m-%d
    :param include_end_date: bool, include end date or not, default False
    :return: list, list of date range
    """

    start_date, end_date = str(start_date), str(end_date)

    try:
        start_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    if include_end_date:
        end_date += timedelta(days=1)

    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    return [date.strftime(output_format) for date in date_range]


def get_list_date_before(end_date, delta_days, input_format="%Y%m%d"):
    """
    Get list of date range delta_days before end_date, [...., end_date). Exclusive end_date

    :param end_date: str, end date. Ex: 20190101 or 2019-01-01
    :param delta_days: int, delta days
    :param input_format: str, date format. Ex: %Y%m%d or %Y-%m-%d
    :return: list, list of date range with default format is %Y%m%d
    """
    start_date = get_previous_date(
        date=end_date,
        delta_days=delta_days,
        input_format=input_format,
        output_format=input_format,
    )

    return get_list_date_range(start_date, end_date, output_format=input_format)


def get_duration_days(start_time, end_time):
    """
    Get duration days

    :param start_time: str, start time. Ex: 2019-01-01 00:00:00
    :param end_time: str, end time. Ex: 2019-01-01 00:00:00
    :return: int, duration days
    """
    try:
        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    except ValueError:
        try:
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%Y%m%d")
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y%m%d")

        except ValueError:
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%Y-%m-%d")
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y-%m-%d")

    duration = end_time - start_time
    return duration.days


def get_duration_hours(start_time, end_time):
    """
    Get duration hours

    :param start_time: str, start time. Ex: 2019-01-01 00:00:00
    :param end_time: str, end time. Ex: 2019-01-01 00:00:00
    :return: float, duration hours
    """
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    duration = end_time - start_time
    return duration.total_seconds() / 3600


def get_previous_date(
    date, delta_days=1, input_format="%Y%m%d", output_format="%Y%m%d"
):
    """
    Get previous date

    :param date: int, str, date in int. Ex: 20190101
    :param delta_days: int, delta days
    :param input_format: str, input format
    :param output_format: str, output format
    :return: str, previous date in str
    """
    date = datetime.strptime(str(date), input_format)
    return (date - timedelta(days=delta_days)).strftime(output_format)


def get_next_date(date, delta_days=1, input_format="%Y%m%d", output_format="%Y%m%d"):
    """
    Get next date

    :param date: int, str, date in int. Ex: 20190101
    :param delta_days: int, delta days
    :param input_format: str, input format
    :param output_format: str, output format
    :return: str, next date in str
    """
    date = datetime.strptime(str(date), input_format)
    return (date + timedelta(days=delta_days)).strftime(output_format)


def get_previous_month(
    date, delta_months=1, input_format="%Y%m%d", output_format="%Y%m%d"
):
    """
    Get previous month

    :param date: int, str, date in int. Ex: 20190101
    :param delta_months: int, delta months
    :param input_format: str, input format
    :param output_format: str, output format
    :return: str, previous month in str
    """
    date = datetime.strptime(str(date), input_format)
    return (date - relativedelta(months=delta_months)).strftime(output_format)


def get_lack_dates(start_date, end_date, list_date):
    """
    Get lack date from start_date to end_date in list_date. Exclusive end_date

    :param start_date: str, start date. Ex: 20190101 or 2019-01-01
    :param end_date: str, end date. Ex: 20190101 or 2019-01-01
    :param list_date: list, list of date
    :return: list, list of lack date
    """
    list_date_range = get_list_date_range(start_date, end_date)
    return list(set(list_date_range) - set(list_date))


def convert_date_to_month(start_date, end_date, output_format="%Y-%m"):
    """
    Convert date to month

    :param start_date: str, start date. Ex: 20190101 or 2019-01-01
    :param end_date: str, end date. Ex: 20190101 or 2019-01-01
    :param output_format: str, output format
    :return: list, list of month
    """

    list_date_range = get_list_date_range(start_date, end_date, include_end_date=True)
    return [datetime.strptime(date, "%Y%m%d").strftime(output_format) for date in list_date_range]
