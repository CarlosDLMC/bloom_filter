from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta


def get_full_path(root_path, year, month, day):
    def parse_day_or_month(element):
        if isinstance(element, list):
            if not element:
                raise ValueError("The list of days/months can not be empty")
            if len(element) == 1:
                return element[0]
            return "{" + ','.join(element) + "}"
        if isinstance(element, int):
            return f"{element:02}"
        return element

    def parse_year(year):
        if isinstance(year, list):
            if not year:
                raise ValueError("The list of years can not be empty")
            if len(year) == 1:
                return year[0]
            return "{" + ','.join(year) + "}"
        return year

    return f"{root_path}/{parse_year(year)}/{parse_day_or_month(month)}/{parse_day_or_month(day)}/*/"


def days_range(start_day, end_day):
    while start_day < end_day:
        yield start_day
        start_day += timedelta(days=1)


def list_days_untill_end_of_month(day_date):
    next_month = day_date + relativedelta(months=1)
    end_date_dt = date(next_month.year, next_month.month, 1)
    return list(f"{date_el.day:02}" for date_el in days_range(day_date, end_date_dt))


def list_days_from_beggining_of_month(day_date):
    last_day = day_date.day
    return list(f"{day:02}" for day in range(1, last_day + 1))


def list_months_untill_end_of_year(month_date):
    return list(f"{month:02}" for month in range(month_date.month, 13))


def list_months_from_beggining_of_year(month_date):
    return list(f"{month:02}" for month in range(1, month_date.month + 1))


def get_date_object_from_input(date_input):
    if isinstance(date_input, str):
        return datetime.strptime(date_input, "%Y-%m-%d").date()
    if isinstance(date_input, datetime):
        return date_input.date()
    return date_input


def get_paths(root_path, start_date, end_date):
    start_date_dt = get_date_object_from_input(start_date)
    end_date_dt = get_date_object_from_input(end_date)

    if start_date_dt > end_date_dt:
        raise ValueError("Error: start_date cannot be later than end_date")

    if start_date_dt.year == end_date_dt.year and start_date_dt.month == end_date_dt.month:
        days = [f"{day:02}" for day in range(start_date_dt.day, end_date_dt.day + 1)]
        return [get_full_path(root_path, start_date_dt.year, start_date_dt.month, days)]
    first_month_path = get_full_path(root_path, start_date_dt.year, start_date_dt.month,
                                     list_days_untill_end_of_month(start_date_dt))
    last_month_path = get_full_path(root_path, end_date_dt.year, end_date_dt.month,
                                    list_days_from_beggining_of_month(end_date_dt))
    first_middle_month = start_date_dt + relativedelta(months=1)
    last_middle_month = end_date_dt - relativedelta(months=1)
    if last_middle_month < first_middle_month:
        return [first_month_path, last_month_path]
    if first_middle_month.year == last_middle_month.year:
        months = [f"{month:02}" for month in range(first_middle_month.month, last_middle_month.month + 1)]
        middle_months_path = get_full_path(root_path, first_middle_month.year, months, "*")
        return [first_month_path, middle_months_path, last_month_path]
    first_year_months = list_months_untill_end_of_year(first_middle_month)
    first_year_months_path = get_full_path(root_path, first_middle_month.year, first_year_months, "*")
    last_year_months = list_months_from_beggining_of_year(last_middle_month)
    last_year_months_path = get_full_path(root_path, last_middle_month.year, last_year_months, "*")
    middle_years = [str(year) for year in range(first_middle_month.year + 1, last_middle_month.year)]
    if not middle_years:
        return [first_month_path, first_year_months_path, last_year_months_path, last_month_path]
    middle_years_path = get_full_path(root_path, middle_years, "*", "*")
    return [first_month_path, first_year_months_path, middle_years_path, last_year_months_path, last_month_path]
