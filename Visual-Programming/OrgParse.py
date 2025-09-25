# Initial Imports
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event
from gcsa.recurrence import Recurrence, DAILY, SU, SA

from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import GridRangeType
from gcsa.calendar import Calendar

import time
import numpy as np
import pandas as pd
import datetime as datetime
from datetime import date, timedelta
import gspread
from beautiful_date import Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sept, Oct, Nov, Dec
from BotScript import client, update_or_create_discord_event, eastern, bot_ready_event
import asyncio
import datetime
import os
from dotenv import load_dotenv
import sys
import asyncio


# Defining Coloring Scheme for GCal (Numbers given from gcsa documentation)
H_color = 10
A_color = 9
L_color = 4
P_color = 6
S_color = 5
MANDATORY_color = 3
SpecialE_color = 8
Missing_color = 1

# def conversion_excel_date(f):
#     temp = datetime.datetime(1899, 12, 30)
#     return temp + datetime.timedelta(f)

def conversion_excel_date(f):
    """
    Converts an Excel serial date number to a datetime.datetime object.
    Handles both integer (date only) and float (date with time) values.
    """
    if not isinstance(f, (int, float)):
        # Handle cases where the input is not a number, e.g., a date string
        # You may need to add more robust handling here based on your data
        try:
            # Try to convert a string to a number
            f = float(f)
        except (ValueError, TypeError):
            # If conversion fails, return None or raise an error
            return None

    # Excel's epoch is Dec 30, 1899
    temp = datetime.datetime(1899, 12, 30)

    # Separate the integer (days) and fractional (time) parts
    days = int(f)
    fractional_days = f - days

    # Calculate timedelta for the full days and the fractional part
    full_days_td = datetime.timedelta(days=days)
    seconds_in_day = 24 * 60 * 60
    seconds_td = datetime.timedelta(seconds=fractional_days * seconds_in_day)

    return temp + full_days_td + seconds_td

def parse_times(Dates, List_Times):

    for j in range(0, len(List_Times)):
        if isinstance(Dates[j], datetime.datetime):
            time_value = List_Times[j]
            if isinstance(time_value, (int, float)):
                excel_time_float = time_value
                total_hours_float = excel_time_float * 24
                hour = int(np.floor(total_hours_float))
                minute = int(60 * (total_hours_float - hour))

                if minute > 59: # Handles issues with floating times
                    minute = 0
                    hour += 1

                List_Times[j] = eastern.localize(
                    datetime.datetime(Dates[j].year, Dates[j].month, Dates[j].day, hour, minute))
            elif isinstance(time_value, str) and time_value.strip() not in ('', 'TBA'):
                try:
                    parsed_time = datetime.datetime.strptime(time_value.strip(), '%I:%M %p').time()
                except ValueError:
                    try:
                        parsed_time = datetime.datetime.strptime(time_value.strip(), '%H:%M').time()
                    except ValueError:
                        print(f"Warning: Could not parse Start Time '{time_value}' for row {j}. Setting to None.")
                        List_Times[j] = None
                        continue
                List_Times[j] = eastern.localize(
                    datetime.datetime(Dates[j].year, Dates[j].month, Dates[j].day, parsed_time.hour,
                                        parsed_time.minute))
            else:
                List_Times[j] = None
        else:
            List_Times[j] = None
    return List_Times

def get_color(Categories):
    Colors = []
    for j in range(0, len(Categories)):
        if (Categories[j] == 'H'):
            Colors.append(H_color)
        elif (Categories[j] == 'A'):
            Colors.append(A_color)
        elif (Categories[j] == 'L'):
            Colors.append(L_color)
        elif (Categories[j] == 'P'):
            Colors.append(P_color)
        elif (Categories[j] == 'S'):
            Colors.append(S_color)
        elif (Categories[j] == 'MANDATORY'):
            Colors.append(MANDATORY_color)
        elif (Categories[j] == 'Special Event!'):
            Colors.append(SpecialE_color)
        else:
            Colors.append(Missing_color)
    return Colors

def clear_dates(Dates):
    cal_day1 = None
    for d in Dates:
        if isinstance(d, datetime.datetime):
            cal_day1 = d
            break
    if cal_day1 is None:
        print("No valid start date found in the calendar data. Cannot clear old events.")
        return
    today = datetime.datetime.today()
    Initial_date = cal_day1 if today.date() < cal_day1.date() else eastern.localize(
        datetime.datetime(today.year, today.month, today.day, today.hour, today.minute))

    last_event_date = None
    for d in reversed(Dates):
        if isinstance(d, datetime.datetime):
            last_event_date = d
            break
    if last_event_date is None:
        Final_date = cal_day1 + datetime.timedelta(days=7)
    else:
        Final_date = eastern.localize(last_event_date + datetime.timedelta(days=1))

    return Initial_date, Final_date

async def post_events(calendar, p):
    Titles, Leaders, Leaders_mask, Dates, Start_Times, End_Times, Locations, Locations_mask, Descriptions, Descriptions_mask, Categories, Colors = p
    for j in range(0, len(Dates)):
        if isinstance(Dates[j], datetime.datetime) and \
                isinstance(Start_Times[j], datetime.datetime) and \
                isinstance(End_Times[j], datetime.datetime) and \
                Titles[j] != '' and Titles[j] is not None:

            location_val = Locations[j] if j < len(Locations) and not Locations_mask[
                j] else 'Check Discord for Location!'
            description_val = Descriptions[j] if j < len(Descriptions) and not Descriptions_mask[
                j] else "It's a surprise!"
            leader_val = Leaders[j] if j < len(Leaders) and not Leaders_mask[j] else 'EBCAO Staff'
            category_val = Categories[j] if j < len(Categories) else 'Unknown'

            if End_Times[j] <= Start_Times[j]:
                print(
                    f"Skipping event '{Titles[j]}' (row {j}) as end time is not after start time: Start={Start_Times[j]}, End={End_Times[j]}")
                continue

            gc_event = Event(
                Titles[j],
                start=Start_Times[j],
                end=End_Times[j],
                location=location_val,
                description=f'<b>Description: </b>{description_val} \n \n<b>Led by: </b>{leader_val} \n \n<b>Category: </b>{category_val}',
                color_id=Colors[j] if j < len(Colors) else Missing_color,
                minutes_before_popup_reminder=30
            )

            await update_or_create_discord_event(
                Titles[j],
                description_val,
                Start_Times[j],
                End_Times[j],
                location_val
            )

            current_time_eastern = eastern.localize(datetime.datetime.now())
            if Start_Times[j] > current_time_eastern:
                print(f'Adding event to Google Calendar: {Titles[j]} (Start Time: {Start_Times[j]})')
                calendar.add_event(gc_event)
            else:
                print(
                    f'Google Calendar event not posted: {Titles[j]} (Start Time: {Start_Times[j]}) since event time has passed.')
        else:
            print(
                f"Skipping row {j} due to missing data: Date={Dates[j]}, Start_Time={Start_Times[j]}, End_Time={End_Times[j]}, Title={Titles[j]}")

