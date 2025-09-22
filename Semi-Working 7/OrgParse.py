# Initial Imports
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event
from gcsa.recurrence import Recurrence, DAILY, SU, SA
import re
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import GridRangeType
from gcsa.calendar import Calendar
import uuid

import time
import numpy as np
import pandas as pd
import datetime as datetime
from datetime import date, timedelta, time
import gspread
from beautiful_date import Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sept, Oct, Nov, Dec
from BotScript import update_or_create_discord_event, eastern
import asyncio
import datetime
import os
from dotenv import load_dotenv
import sys
import asyncio
import json
from gspread.exceptions import APIError

# from FSI_Programming import Import_Prog, Reorganize_Sheet, Verbose_Sheet

# Defining Coloring Scheme for GCal (Numbers given from gcsa documentation)
H_color = 10
A_color = 9
L_color = 4
P_color = 6
S_color = 5
MANDATORY_color = 3
SpecialE_color = 8
Missing_color = 1

EVENT_DATA_FILE = 'events.json'

def conversion_excel_date(f):
    temp = datetime.datetime(1899, 12, 30)
    return temp + datetime.timedelta(f)

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

async def post_events(bot, wks, week_number, IDCol, program, calendar, p):
    Titles, Leaders, Leaders_mask, Dates, Start_Times, End_Times, Locations, Locations_mask, Descriptions, Descriptions_mask, Categories, Event_IDs, Colors = p
    events = []
    try:
        with open(EVENT_DATA_FILE, 'r') as f:
            events = json.load(f)
    except FileNotFoundError:
        print("Event data file not found. Starting with an empty list.")
    
    for j in range(0, len(Dates)):
        if isinstance(Dates[j], datetime.datetime) and \
                isinstance(Start_Times[j], datetime.datetime) and \
                isinstance(End_Times[j], datetime.datetime) and \
                Titles[j] != '' and Titles[j] is not None:

            location_val = Locations[j] if j < len(Locations) and not Locations_mask[j] else 'Check Discord for Location!'
            description_val = Descriptions[j] if j < len(Descriptions) and not Descriptions_mask[j] else "It's a surprise!"
            leader_val = Leaders[j] if j < len(Leaders) and not Leaders_mask[j] else 'EBCAO Staff'
            category_val = Categories[j] if j < len(Categories) else 'Unknown'

            if End_Times[j] <= Start_Times[j]:
                print(f"Skipping event '{Titles[j]}' (row {j}) as end time is not after start time: Start={Start_Times[j]}, End={End_Times[j]}")
                continue
            
            process = None
            print(Event_IDs)
            if (Event_IDs[j] == '' or Event_IDs[j] is None):
                process = "Creation"
                event_id = str(uuid.uuid4())
                wks.update_cell(j+4, IDCol+2, event_id) 
                event = {
                    "title": Titles[j], "date": Dates[j].isoformat(), 
                    "start_time": Start_Times[j].isoformat(), "end_time": End_Times[j].isoformat(),
                    "week": week_number,
                    "description": description_val,
                    "location": location_val, "leaders": leader_val, "category": Categories[j],
                    "recording": None, "id": event_id, "discord_id": 0,
                    "calendar_id": 0, "status": "Active"
                }
                events.append(event)
            else:
                print(f"Event ID :{Event_IDs[j]}")
                process = "Update"
                event = None
                for event_j in events:
                    if (Event_IDs[j] == event_j["id"]):
                        event = event_j
                        break

                if event:
                    event["title"] = Titles[j]
                    event["date"] = Dates[j].isoformat()
                    event["start_time"] = Start_Times[j].isoformat()
                    event["end_time"] = End_Times[j].isoformat()
                    event["status"] = "Active"
                else:
                    print(f"Error: Could not find event with ID {Event_IDs[j]} to update.")
            
            gc_event = Event(
                Titles[j], start=Start_Times[j], end=End_Times[j],
                location=location_val,
                description=f'<b>Description: </b>{description_val} \n \n<b>Led by: </b>{leader_val} \n \n<b>Category: </b>{category_val}',
                color_id=Colors[j] if j < len(Colors) else Missing_color,
                minutes_before_popup_reminder=30
            )
            
            if(process == "Creation"):
                current_time_eastern = eastern.localize(datetime.datetime.now())
                if Start_Times[j] > current_time_eastern:
                    print(f'Adding event to Google Calendar: {Titles[j]} (Start Time: {Start_Times[j]})')
                    created_event = calendar.add_event(gc_event)
                    calendar_id = created_event.event_id
                    discord_id = await update_or_create_discord_event(bot, program, Titles[j], f'**Description:** {description_val} \n \n**Led by:** {leader_val} \n \n**Category:** {category_val}', Start_Times[j], End_Times[j], location_val)
                    event["calendar_id"] = calendar_id
                    event["discord_id"] = discord_id
                else:
                    print(f'Google Calendar event not posted: {Titles[j]} (Start Time: {Start_Times[j]}) since event time has passed.')
            elif(process == "Update"):
                if(event["status"] == "Active"):
                    gc_event = calendar.get_event(event_id=event["calendar_id"])
                    gc_event.summary = Titles[j]
                    gc_event.start = Start_Times[j]
                    gc_event.end = End_Times[j]
                    gc_event.location = location_val
                    gc_event.description = f'<b>Description: </b>{description_val} \n \n<b>Led by: </b>{leader_val} \n \n<b>Category: </b>{category_val}'
                    gc_event.color_id = Colors[j] if j < len(Colors) else Missing_color
                    gc_event.minutes_before_popup_reminder = 30
                    gc_event.event_id = event["calendar_id"]
                else:
                    calendar.delete_event(event["calendar_id"])
                discord_id = await update_or_create_discord_event(bot, program, Titles[j], description_val, Start_Times[j], End_Times[j], location_val, event["discord_id"], event["status"])
        else:
            print(f"Skipping row {j} due to missing data: Date={Dates[j]}, Start_Time={Start_Times[j]}, End_Time={End_Times[j]}, Title={Titles[j]}")
    with open(EVENT_DATA_FILE, 'w') as f:
        json.dump(events, f, indent=4)

def Sort_Events(events):
    # Sort the events by 'date' and then by 'start_time'
    events.sort(key=lambda x: (datetime.datetime.fromisoformat(x['date']).date(), datetime.datetime.fromisoformat(x['start_time']).time()))
    return events

async def update_events_by_id(bot, wks, program, calendar, event_ID, update_args=None):
    import datetime as _dt
    import json

    events = []
    try:
        with open(EVENT_DATA_FILE, 'r') as f:
            events = json.load(f)
    except FileNotFoundError:
        print("Event data file not found. Starting with an empty list.")

    event0 = None
    for event in events:
        if event["id"] == event_ID:
            event0 = event
            print("Found Event to Update")
            break

    if event0 is None:
        print("No matching event.")
        return

    if update_args is None:
        print("No updates provided, aborting.")
        return

    # ----- update internal event dict (unchanged logic) -----
    if update_args.get("title") is not None:
        event0["title"] = update_args["title"]

    if "date" in update_args:
        existing_start_time = _dt.datetime.fromisoformat(event0["start_time"]).time()
        existing_end_time   = _dt.datetime.fromisoformat(event0["end_time"]).time()
        new_start_dt = eastern.localize(_dt.datetime.combine(update_args["date"], existing_start_time))
        new_end_dt   = eastern.localize(_dt.datetime.combine(update_args["date"], existing_end_time))
        event0["start_time"] = new_start_dt.isoformat()
        event0["end_time"]   = new_end_dt.isoformat()
        event0["date"]       = new_start_dt.isoformat()

    if "start_time" in update_args:
        existing_date = _dt.datetime.fromisoformat(event0["date"]).date()
        new_start_dt = eastern.localize(_dt.datetime.combine(existing_date, update_args["start_time"]))
        event0["start_time"] = new_start_dt.isoformat()

    if "end_time" in update_args:
        existing_date = _dt.datetime.fromisoformat(event0["date"]).date()
        new_end_dt = eastern.localize(_dt.datetime.combine(existing_date, update_args["end_time"]))
        event0["end_time"] = new_end_dt.isoformat()

    for key in ("leaders","location","category","description","recording","status"):
        if update_args.get(key) is not None:
            event0[key] = update_args[key]

    print("Successfully Updated Internal Memory of Event")

    # ----- push to GCal / Discord (unchanged flow) -----
    start_time_date = _dt.datetime.fromisoformat(event0["start_time"])
    end_time_date   = _dt.datetime.fromisoformat(event0["end_time"])

    if event0["status"] == "Active":
        gc_event = calendar.get_event(event_id=event0["calendar_id"])
        gc_event.summary = event0["title"]
        gc_event.start   = start_time_date
        gc_event.end     = end_time_date
        gc_event.location = event0["location"]
        gc_event.description = (
            f'<b>Description: </b>{event0["description"]} \n \n'
            f'<b>Led by: </b>{event0["leaders"]} \n \n'
            f'<b>Category: </b>{event0["category"]}'
        )
        gc_event.minutes_before_popup_reminder = 30
        calendar.update_event(gc_event)
    else:
        calendar.delete_event(event0["calendar_id"])

    await update_or_create_discord_event(
        bot, program, event0["title"],
        f'**Description:** {event0["description"]} \n \n**Led by:** {event0["leaders"]} \n \n**Category:** {event0["category"]}',
        start_time_date, end_time_date, event0["location"], event0["discord_id"], event0["status"]
    )

    # ----- update the SOG sheet row (THIS IS THE FIXED PART) -----
    week_number = int(event0.get("week", 0))
    df = pd.DataFrame(wks.get_worksheet(week_number+2).get_all_values(value_render_option='UNFORMATTED_VALUE'))[2:][:]
    headers = df.iloc[0].values
    df.columns = headers
    df = df[1:]
    event_ids = df['Event ID'].tolist()
    ii = None
    for i, eid in enumerate(event_ids):
        if eid == event0["id"]:
            ii = i
            break

    ws = wks.get_worksheet(week_number+2)

    # Make sure the sheet is in the "date1,date1,..." state (unmerge + filldown)
    try:
        filldown_dates_in_sheet(ws)
    except Exception as e:
        print(f"[warn] filldown_dates_in_sheet in update_events_by_id failed: {e}")

    if ii is not None:
        row = ii + 4  # data starts at row 4
        # human-readable date string (text, not serial)
        date_str = start_time_date.strftime('%A, %B %d')

        # Write with USER_ENTERED so Sheets keeps text and doesn’t coerce to serial
        ws.update(f"A{row}:A{row}", [[date_str]], value_input_option='USER_ENTERED')
        ws.update(f"B{row}:B{row}", [["Updated Details!"]], value_input_option='USER_ENTERED')
        ws.update(f"C{row}:C{row}", [[event0["title"]]], value_input_option='USER_ENTERED')
        ws.update(f"D{row}:D{row}", [[event0["leaders"]]], value_input_option='USER_ENTERED')
        ws.update(f"E{row}:E{row}", [[start_time_date.strftime('%I:%M %p').lstrip('0')]], value_input_option='USER_ENTERED')
        ws.update(f"F{row}:F{row}", [[end_time_date.strftime('%I:%M %p').lstrip('0')]], value_input_option='USER_ENTERED')
        ws.update(f"G{row}:G{row}", [[event0["description"]]], value_input_option='USER_ENTERED')
        ws.update(f"H{row}:H{row}", [[event0["location"]]], value_input_option='USER_ENTERED')
        ws.update(f"J{row}:J{row}", [[event0["category"]]], value_input_option='USER_ENTERED')
        ws.update(f"K{row}:K{row}", [[event0["recording"] or ""]], value_input_option='USER_ENTERED')

    with open(EVENT_DATA_FILE, 'w') as f:
        json.dump(events, f, indent=4)

    # Reorganize after writing (this will re-merge visually, but underlying data stays filled)
    Organize_Sheet(ws, wks)

def get_events_from_file():
    try:
        with open(EVENT_DATA_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def get_event_by_search_query(search_query):
    events = get_events_from_file()

    try:
        discord_id = int(search_query)
        matching_event = next((e for e in events if e.get('discord_id') == discord_id), None)
        if matching_event:
            return matching_event
    except (ValueError, TypeError):
        pass

    try:
        if isinstance(search_query, str) and len(search_query) == 36 and uuid.UUID(search_query):
            matching_event = next((e for e in events if e.get('id') == search_query), None)
            if matching_event:
                return matching_event
    except ValueError:
        pass

    try:
        if isinstance(search_query, str):
            matching_event = next((e for e in events if e.get('calendar_id') == search_query), None)
            if matching_event:
                return matching_event
    except (ValueError, TypeError):
        pass

    matching_events_by_title = [e for e in events if e.get('title', '').lower() == search_query.lower()]
    if matching_events_by_title:
        return matching_events_by_title

    return None

def get_event_submitted(wks_prog, search_query: str):
    try:
        data = wks_prog.get_all_records()  # Get all data as a list of dictionaries
    except Exception as e:
        print(f"Error fetching data from submitted events sheet: {e}")
        return []

    matching_events = [
        row for row in data 
        if row.get('Event Title', '').lower() == search_query.lower()
    ]

    return matching_events if matching_events else None

def update_events_submitted(wks_prog, event_to_edit: dict, update_args: dict) -> None:
    try:
        all_values = wks_prog.get_all_values()
        headers = all_values[0]
        col_indices = {header: headers.index(header) + 1 for header in headers}
        
        row_index = -1
        for i, row in enumerate(all_values):
            # Check the event title and date using the keys from the dictionary returned by get_all_records()
            if row[col_indices['Event Title'] - 1] == event_to_edit.get('Event Title') and \
               row[col_indices['Event Date'] - 1] == event_to_edit.get('Event Date'):
                row_index = i + 1  # Get the 1-based index for gspread
                break
        
        if row_index == -1:
            print("Error: Could not find the specific event to update.")
            return

        updates = []
        for key, value in update_args.items():
            gspread_col_name = {
                "title": "Event Title",
                "date": "Event Date", # Changed from "Date" to "Event Date" for consistency
                "start_time": "Start Time",
                "end_time": "End Time",
                "hosts": "Host & CoHosts",
                "description": "Event Description",
                "halps": "Suggested HALPS Category",
                "location": "Location",
            }.get(key)
            
            if gspread_col_name and gspread_col_name in col_indices:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, col_indices[gspread_col_name]),
                    'values': [[value]]
                })

        if updates:
            wks_prog.batch_update(updates)
            print(f"Successfully updated submitted event at row {row_index}.")
        else:
            print("No valid updates to perform.")

    except gspread.exceptions.APIError as e:
        print(f"Error during Google Sheets API call: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def prog_weeks(Weeks_arr):
    ii_w = []
    i0 = 0
    i1 = 0
    for i in range(0, len(Weeks_arr)):
        if(i == 0):
            i0 = i
        elif(Weeks_arr[i] != '' and not (isinstance(Weeks_arr[i],int))):
            ii_w.append([i0, i1])
            i0 = i
        else:
            i1 = i
    if i0 <= i1:
        ii_w.append([i0, i1])
    return ii_w

def sog_days(Dates_arr_SOG):
    ii_d_SOG = []
    i0 = 0
    i1 = 0
    for j in range(0, len(Dates_arr_SOG)):
        if (j == 0):
            i0 = j
        elif (Dates_arr_SOG[j] != ''):
            if(j == i0+1 and Dates_arr_SOG[i0] != ''):
                ii_d_SOG.append([i0, i0])
            elif (Dates_arr_SOG[j] == 'Ongoing Challenges'):
                ii_d_SOG.append([i0, i1-1])
            else:
                ii_d_SOG.append([i0, i1])
            i0 = j
        else:
            i1 = j
    if i0 <= i1:
            ii_d_SOG.append([i0, i1])
    return ii_d_SOG

def get_programming(cal_data, ii):
    Date_arr = cal_data["Date"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Start_arr = cal_data["Start Time"][ii[0]:ii[1] + 1].reset_index(drop=True)
    End_arr = cal_data["End Time"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Host_arr = cal_data["Host"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Name_arr = cal_data["Name"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Description_arr = cal_data["Description"][ii[0]:ii[1] + 1].reset_index(drop=True)
    HALPS_arr = cal_data["HALPS Category"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Location_arr = cal_data["Location"][ii[0]:ii[1] + 1].reset_index(drop=True)
    return Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr

def clean_headers(raw_headers_list, prefix="Unnamed"):
    cleaned = []
    seen_headers = {}
    for i, h in enumerate(raw_headers_list):
        header_str = str(h).strip()
        if not header_str:
            header_str = f"{prefix}_{i}"
        original_header_str = header_str
        count = seen_headers.get(original_header_str, 0)
        if count > 0:
            header_str = f"{original_header_str}_{count}"
        seen_headers[original_header_str] = count + 1
        cleaned.append(header_str)
    return cleaned

def Import_Prog(program, wks, wks_SOG, week_number, PROGRAMMING):
    if(PROGRAMMING == 0):
        Import_Sheet(program, wks, wks_SOG, week_number, PROGRAMMING)
    elif(PROGRAMMING == 1):
        Import_Sheet(program, wks, wks_SOG, week_number, PROGRAMMING)
    else:
        Import_Sheet(program, wks, wks_SOG, week_number, 0)
        Import_Sheet(program, wks, wks_SOG, week_number, 1)
        
def Import_Sheet(program, wks, wks_SOG, week_number, PROGRAMMING):
    cal_data = pd.DataFrame(wks.get_worksheet(PROGRAMMING).get_all_values(value_render_option='UNFORMATTED_VALUE'))[0:][:]
    headers = cal_data.iloc[0].values
    cal_data.columns = headers
    cal_data = cal_data[1:].reset_index(drop=True)
    Weeks_arr = cal_data[headers[0]]
    ii_w = prog_weeks(Weeks_arr)

    print(f'Printing events for Week #{week_number} from Programming sheet #{PROGRAMMING}...')

    Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr = get_programming(cal_data, ii_w[week_number-1])

    worksheet_SOG_index = 2 + week_number
    worksheet_SOG = wks_SOG.get_worksheet(worksheet_SOG_index)
    sog_header_row_gspread_idx = 2
    sog_data_start_row_gspread_idx = sog_header_row_gspread_idx + 1

    full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
    headers_SOG_raw = full_sog_values[sog_header_row_gspread_idx]

    while len(headers_SOG_raw) <= 13:
        headers_SOG_raw.append('')

    stored_column_M_values = [row[12] if len(row) > 12 else '' for row in full_sog_values[sog_data_start_row_gspread_idx:]]
    stored_column_N_values = [row[13] if len(row) > 13 else '' for row in full_sog_values[sog_data_start_row_gspread_idx:]]
    
    print(f"Stored {len(stored_column_M_values)} values for column M.")
    print(f"Stored {len(stored_column_N_values)} values for column N.")

    cal_data_SOG = pd.DataFrame(worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE'))[sog_header_row_gspread_idx:][:]
    cal_data_SOG.columns = cal_data_SOG.iloc[0].values
    cal_data_SOG = cal_data_SOG[1:].reset_index(drop=True)
    current_df_headers_SOG = cal_data_SOG.columns.tolist()
    
    date_col_header = current_df_headers_SOG[0] if len(current_df_headers_SOG) > 0 else 'Column1'
    Dates_arr_SOG = cal_data_SOG[date_col_header]
    ii_d_SOG = sog_days(Dates_arr_SOG)

    for j in range(len(Date_arr)):
        current_input_date = Date_arr[j]
        current_input_name = Name_arr[j]
        sog_day_block_index = -1
        for k in range(len(ii_d_SOG)):
            first_row_of_block_df_idx = ii_d_SOG[k][0]
            name_col_header = current_df_headers_SOG[2] if len(current_df_headers_SOG) > 2 else 'Column3'
            if first_row_of_block_df_idx < len(Dates_arr_SOG) and Dates_arr_SOG.iloc[first_row_of_block_df_idx] == current_input_date:
                sog_day_block_index = k
                break
        
        if sog_day_block_index == -1:
            print(f"Warning: Date {current_input_date} not found in SOG sheet index {worksheet_SOG_index}. Skipping event '{current_input_name}'.")
            continue

        day_start_df_idx, day_end_df_idx = ii_d_SOG[sog_day_block_index]
        Name_arr_SOG = cal_data_SOG[name_col_header][day_start_df_idx : day_end_df_idx + 1].reset_index(drop=True)

        match_found_at_sog_df_index = -1
        for l in range(len(Name_arr_SOG)):
            if Name_arr_SOG[l] == current_input_name:
                match_found_at_sog_df_index = l
                break
        
        new_row_data = [
            current_input_name, Host_arr[j], Start_arr[j], End_arr[j], 
            Description_arr[j], Location_arr[j], 1, HALPS_arr[j]
        ]
    
        if match_found_at_sog_df_index != -1:
            print(f"Updating Event: '{current_input_name}'")
            update_row_sheet = day_start_df_idx + match_found_at_sog_df_index + sog_data_start_row_gspread_idx + 1
            print(f"  -> Found at SOG DataFrame index: {day_start_df_idx + match_found_at_sog_df_index}, Updating Sheet row: {update_row_sheet}")
            range_for_row = f"C{update_row_sheet}:J{update_row_sheet}"
            try:
                worksheet_SOG.update(range_for_row, [new_row_data])
                print(f"  -> Successfully updated event '{current_input_name}' in sheet.")
            except APIError as e:
                print(f"  -> Error updating row {update_row_sheet} for event '{current_input_name}': {e.response.text}")
        else:
            print(f"Creating Event: '{current_input_name}'")
            insert_row_sheet = day_end_df_idx + sog_data_start_row_gspread_idx + 1 + 1
            print(f"  -> Inserting data at Sheet row: {insert_row_sheet}")
            insert_row_data_full = ['', ''] + new_row_data
            try:
                worksheet_SOG.insert_row(insert_row_data_full, index=insert_row_sheet)
                print(f"  -> Successfully inserted new event '{current_input_name}'.")
            except APIError as e:
                print(f"  -> Error inserting row at {insert_row_sheet} for event '{current_input_name}': {e.response.text}")
            
            worksheet_id = worksheet_SOG.id
            source_row_1_indexed = insert_row_sheet - 1
            destination_row_1_indexed = insert_row_sheet
            source_start_row_api = source_row_1_indexed - 1
            source_end_row_api = source_row_1_indexed
            destination_start_row_api = destination_row_1_indexed - 1
            destination_end_row_api = destination_row_1_indexed
            copy_up_to_column_exclusive_index = 10
            requests = [{
                "copyPaste": {
                    "source": { "sheetId": worksheet_id, "startRowIndex": source_start_row_api, "endRowIndex": source_end_row_api, "startColumnIndex": 0, "endColumnIndex": copy_up_to_column_exclusive_index },
                    "destination": { "sheetId": worksheet_id, "startRowIndex": destination_start_row_api, "endRowIndex": destination_end_row_api, "startColumnIndex": 0, "endColumnIndex": copy_up_to_column_exclusive_index },
                    "pasteOrientation": "HORIZONTAL", "pasteType": "PASTE_FORMAT"
                }
            }]
            try:
                wks_SOG.batch_update({"requests": requests})
                print(f"  -> Successfully sent request to copy style.")
            except APIError as e:
                print(f"  -> Error copying style for row {destination_row_1_indexed}: {e.response.text}")

            print("  -> Re-reading SOG data after insertion to refresh in-memory DataFrame and indices.")
            full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
            cal_data_SOG = pd.DataFrame(full_sog_values[sog_header_row_gspread_idx:][:], columns=headers_SOG_raw)
            cal_data_SOG = cal_data_SOG[1:].reset_index(drop=True)
            current_df_headers_SOG = cal_data_SOG.columns.tolist()
            date_col_header = current_df_headers_SOG[0]
            Dates_arr_SOG = cal_data_SOG[date_col_header]
            ii_d_SOG = []
            i0_re = 0; i1_re = 0
            for k_re in range(0, len(Dates_arr_SOG)):
                if (k_re == 0): i0_re = k_re
                elif (Dates_arr_SOG[k_re] != ''):
                    if(k_re == i0_re+1 and Dates_arr_SOG[i0_re] != ''): ii_d_SOG.append([i0_re, i0_re])
                    elif (Dates_arr_SOG[k_re] == 'Ongoing Challenges'): ii_d_SOG.append([i0_re, i1_re-1])
                    else: ii_d_SOG.append([i0_re, i1_re])
                    i0_re = k_re
                else: i1_re = k_re
            if i0_re <= i1_re: ii_d_SOG.append([i0_re, i1_re])
            print(f"  -> ii_d_SOG re-calculated for sheet {worksheet_SOG_index}: {ii_d_SOG}")

    final_full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
    final_data_rows_count = len(final_full_sog_values) - sog_data_start_row_gspread_idx

    if stored_column_M_values is not None:
        if len(stored_column_M_values) < final_data_rows_count: stored_column_M_values.extend([''] * (final_data_rows_count - len(stored_column_M_values)))
        elif len(stored_column_M_values) > final_data_rows_count: stored_column_M_values = stored_column_M_values[:final_data_rows_count]
    else: stored_column_M_values = [''] * final_data_rows_count
    if stored_column_N_values is not None:
        if len(stored_column_N_values) < final_data_rows_count: stored_column_N_values.extend([''] * (final_data_rows_count - len(stored_column_N_values)))
        elif len(stored_column_N_values) > final_data_rows_count: stored_column_N_values = stored_column_N_values[:final_data_rows_count]
    else: stored_column_N_values = [''] * final_data_rows_count

    update_range_start_row = sog_data_start_row_gspread_idx + 1
    m_n_data_for_update = []
    for row_idx in range(final_data_rows_count):
        m_n_data_for_update.append([stored_column_M_values[row_idx], stored_column_N_values[row_idx]])

    if m_n_data_for_update:
        try:
            range_m_n_update = f"M{update_range_start_row}:N{update_range_start_row + final_data_rows_count - 1}"
            worksheet_SOG.update(range_m_n_update, m_n_data_for_update)
            print(f"Successfully re-pasted columns M and N for sheet '{worksheet_SOG.title}'.")
        except APIError as e:
            print(f"Error re-pasting columns M and N for sheet '{worksheet_SOG.title}': {e.response.text}")
    else:
        print(f"No data to re-paste for columns M and N in sheet '{worksheet_SOG.title}'.")
    print('Printing completed.')

def Deduplicate_Headers(headers):
    new_headers = []
    counts = {}
    for header in headers:
        clean_header = str(header).strip() if pd.notna(header) else ''
        if clean_header in counts:
            counts[clean_header] += 1
            new_headers.append(f"{clean_header}.{counts[clean_header]}")
        else:
            counts[clean_header] = 1
            new_headers.append(clean_header)
    return new_headers

def Parse_Dates(cell_value, numeric_date):
    found_strings = re.findall(r'(\w+,\s\w+\s\d+)', str(cell_value))
    if found_strings: return [pd.to_datetime(d, errors='coerce') for d in found_strings]
    if pd.notna(numeric_date):
        try:
            origin = pd.Timestamp('1899-12-30')
            return [origin + pd.to_timedelta(float(numeric_date), unit='D')]
        except (ValueError, TypeError): return []
    return []

def Format_Time(numeric_time):
    if pd.isna(numeric_time): return ""
    try: total_seconds = int(float(numeric_time) * 86400)
    except (ValueError, TypeError): return ""
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours >= 24: hours, minutes = 23, 59
    try:
        t = time(hour=hours, minute=minutes)
        if t.minute == 0: return t.strftime('%-I%p').lower()
        else: return t.strftime('%-I:%M%p').lower()
    except ValueError: return ""

def Organize_Sheet(worksheet, spreadsheet_obj):
    """
    1) Unmerge Date & Notes columns across the data region.
    2) Forward-fill Date (text) in-sheet.
    3) Sort rows by (Date, Start Time) using two temporary numeric helper columns.
    4) Delete helpers.
    5) Re-merge contiguous equal-Date groups for Date and Notes.
    """
    import pandas as pd
    import numpy as np
    import json
    from datetime import datetime
    from gspread.utils import rowcol_to_a1

    print(f"--- Processing sheet: '{worksheet.title}' ---")

    # A) ensure NO vertical merges in Date or Notes before sorting
    try:
        unmerge_columns_in_data(worksheet, header_names=("Date", "Notes"))
    except Exception as e:
        print(f"[warn] unmerge Date/Notes failed: {e}")

    # B) fill-down Date so every row has a value
    try:
        filldown_dates_in_sheet(worksheet)
    except Exception as e:
        print(f"[warn] filldown_dates_in_sheet failed: {e}")

    # C) load grid
    all_values = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if not all_values:
        print(f"Skipping sheet '{worksheet.title}': empty.")
        return

    header_row_idx0 = 2   # row 3
    data_start_idx0 = 3   # row 4

    if len(all_values) <= header_row_idx0:
        print(f"Skipping sheet '{worksheet.title}': no headers.")
        return

    raw_headers = list(all_values[header_row_idx0])
    while raw_headers and raw_headers[-1] == "":
        raw_headers.pop()
    if not raw_headers:
        print(f"Skipping sheet '{worksheet.title}': empty headers.")
        return

    def _dedupe_headers(headers):
        seen, out = {}, []
        for h in headers:
            name = h if h is not None else ""
            if name not in seen:
                seen[name] = 1; out.append(name)
            else:
                seen[name] += 1; out.append(f"{name}_{seen[name]}")
        return out

    headers = _dedupe_headers(raw_headers)
    if len(all_values) <= data_start_idx0:
        print(f"Skipping sheet '{worksheet.title}': no data rows.")
        return

    data_rows = all_values[data_start_idx0:]
    norm_rows = [r[:len(headers)] + [""] * max(0, len(headers) - len(r)) for r in data_rows]
    # Silence the FutureWarning by explicitly calling infer_objects
    df = pd.DataFrame(norm_rows, columns=headers).replace('', np.nan)
    df = df.infer_objects(copy=False)

    def _find_col(name):
        if name in df.columns: return name
        for c in df.columns:
            if str(c).strip().lower() == name.lower(): return c
        return None

    date_col   = _find_col('Date')
    notes_col  = _find_col('Notes')
    start_col  = _find_col('Start Time')
    eid_col    = _find_col('Event ID')

    if date_col is None or notes_col is None or start_col is None:
        print(f"Missing required columns.")
        return

    # D) build sort keys (prefer events.json ISO datetimes)
    try:
        with open('events.json', 'r') as f:
            evmap = {e['id']: e for e in (json.load(f) or []) if isinstance(e, dict) and e.get('id')}
    except FileNotFoundError:
        evmap = {}

    def _parse_time_str(s):
        if not isinstance(s, str): return None
        S = s.strip().upper().replace('.', '')
        for fmt in ("%I:%M %p", "%I %p"):
            try: return datetime.strptime(S, fmt).time()
            except ValueError: pass
        return None

    date_keys, time_keys = [], []
    years_in_events = []
    for e in evmap.values():
        try: years_in_events.append(datetime.fromisoformat(e['start_time']).year)
        except Exception: pass
    inferred_year = max(set(years_in_events), key=years_in_events.count) if years_in_events else datetime.now().year

    for i in range(len(df)):
        d_key = None; t_key = None

        # from events.json via Event ID
        if eid_col is not None:
            eid = df.at[i, eid_col]
            if isinstance(eid, str) and eid in evmap:
                try:
                    st_iso = evmap[eid]['start_time']
                    st = datetime.fromisoformat(st_iso)
                    d_key = st.year * 10000 + st.month * 100 + st.day
                    t_key = st.hour * 60 + st.minute
                except Exception:
                    pass

        # fallback: parse visible Date + Start Time
        if d_key is None:
            txt = df.at[i, date_col]
            if isinstance(txt, str) and txt.strip():
                for fmt in ("%A, %B %d, %Y", "%A, %B %d"):
                    try:
                        dt = datetime.strptime(txt.strip(), fmt)
                        if fmt == "%A, %B %d": dt = dt.replace(year=inferred_year)
                        d_key = dt.year * 10000 + dt.month * 100 + dt.day
                        break
                    except ValueError:
                        continue
        if t_key is None:
            t = _parse_time_str(df.at[i, start_col])
            if t is not None:
                t_key = t.hour * 60 + t.minute

        date_keys.append("" if d_key is None else d_key)
        time_keys.append("" if t_key is None else t_key)

    # E) add two helper cols, sort, remove helpers
    total_cols = len(headers)
    h_date_idx0 = total_cols
    h_time_idx0 = total_cols + 1

    try:
        spreadsheet_obj.batch_update({
            "requests": [{
                "insertDimension": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": h_date_idx0,
                        "endIndex": h_time_idx0 + 1
                    },
                    "inheritFromBefore": True
                }
            }]
        })
    except Exception as e:
        print(f"[warn] insert helpers failed: {e}")

    start_row_1b = data_start_idx0 + 1
    end_row_1b   = data_start_idx0 + len(df)
    a1_date = f"{rowcol_to_a1(start_row_1b, h_date_idx0 + 1)}:{rowcol_to_a1(end_row_1b, h_date_idx0 + 1)}"
    a1_time = f"{rowcol_to_a1(start_row_1b, h_time_idx0 + 1)}:{rowcol_to_a1(end_row_1b, h_time_idx0 + 1)}"
    worksheet.update(a1_date, [[v] for v in date_keys], value_input_option='USER_ENTERED')
    worksheet.update(a1_time, [[v] for v in time_keys], value_input_option='USER_ENTERED')

    # SORT (now safe: Date & Notes are unmerged)
    try:
        spreadsheet_obj.batch_update({
            "requests": [{
                "sortRange": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": data_start_idx0,
                        "endRowIndex": data_start_idx0 + len(df),
                        "startColumnIndex": 0,
                        "endColumnIndex": h_time_idx0 + 1
                    },
                    "sortSpecs": [
                        {"dimensionIndex": h_date_idx0, "sortOrder": "ASCENDING"},
                        {"dimensionIndex": h_time_idx0, "sortOrder": "ASCENDING"},
                    ]
                }
            }]
        })
    except Exception as e:
        print(f"[warn] sortRange failed: {e}")

    # delete helpers
    try:
        spreadsheet_obj.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": h_date_idx0,
                        "endIndex": h_time_idx0 + 1
                    }
                }
            }]
        })
    except Exception as e:
        print(f"[warn] delete helpers failed: {e}")

    # F) reload and re-merge equal-Date groups for Date and Notes
    all_values = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    data_rows = all_values[data_start_idx0:]
    norm_rows = [r[:len(headers)] + [""] * max(0, len(headers) - len(r)) for r in data_rows]
    df = pd.DataFrame(norm_rows, columns=headers).replace('', np.nan)

    date_series = df[date_col].astype(object)
    groups = []
    start = None; prev = None
    for i, val in enumerate(date_series):
        if pd.isna(val):
            if start is not None and i - start >= 2: groups.append((start, i - 1))
            start = None; prev = None; continue
        if prev is None or val != prev:
            if start is not None and i - start >= 2: groups.append((start, i - 1))
            start = i
        prev = val
    if start is not None:
        i = len(date_series)
        if i - start >= 2: groups.append((start, i - 1))

    def _col_idx(label):
        for idx, h in enumerate(headers):
            if str(h).strip().lower() == label.lower(): return idx
        return None

    date_idx0 = _col_idx('date')
    notes_idx0 = _col_idx('notes')
    if date_idx0 is None or notes_idx0 is None:
        print("Cannot locate Date/Notes for merging."); return

    reqs = []
    for r0, r1 in groups:
        top_api = (r0 + 4) - 1
        bot_api = (r1 + 4)
        for cidx in (date_idx0, notes_idx0):
            reqs.append({
                "mergeCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": top_api,
                        "endRowIndex": bot_api,
                        "startColumnIndex": cidx,
                        "endColumnIndex": cidx + 1
                    },
                    "mergeType": "MERGE_ALL"
                }
            })
    if reqs:
        try:
            spreadsheet_obj.batch_update({"requests": reqs})
            print(f"Merged {len(groups)} date groups in '{worksheet.title}'.")
        except Exception as e:
            print(f"[warn] merge requests failed: {e}")

def Verbose_Sheet(program, wks_SOG, week_number):
    specific_week = True
    sog_tab = 2 + week_number
    all_worksheets = wks_SOG.worksheets()
    sheets_to_process = []
    if specific_week:
        if 0 <= sog_tab < len(all_worksheets):
            worksheet_to_add = all_worksheets[sog_tab]
            if worksheet_to_add.title not in ["Welcome!", "Template"]:
                sheets_to_process.append(worksheet_to_add.title)
                print(f"Processing only specified week: '{worksheet_to_add.title}' (Tab Index: {sog_tab})")
            else:
                print(f"Skipping specified worksheet '{worksheet_to_add.title}' (Tab Index: {sog_tab}) as it's a excluded sheet.")
        else:
            print(f"Error: Specified target tab index {sog_tab} is out of bounds for the number of worksheets available ({len(all_worksheets)}).")
            return
    else:
        sheets_to_process = [s.title for s in all_worksheets if s.title not in ["Welcome!", "Template"]]
        print("Processing all sheets except 'Welcome!' and 'Template'.")
    for sheet_name in sheets_to_process:
        worksheet = wks_SOG.worksheet(sheet_name)
        try:
            Organize_Sheet(worksheet, wks_SOG)
        except Exception as e:
            print(f"!!! An error occurred while processing sheet '{sheet_name}': {e}")
    print('\nAll sheets processed.')

def Reorganize_Sheet(program, wks_SOG, week_number):
    specific_week = True
    sog_tab = 2 + week_number
    all_worksheets = wks_SOG.worksheets()
    sheets_to_process = []
    if specific_week:
        if 0 <= sog_tab < len(all_worksheets):
            worksheet_to_add = all_worksheets[sog_tab]
            if worksheet_to_add.title not in ["Welcome!", "Template"]:
                sheets_to_process.append(worksheet_to_add.title)
                print(f"Processing only specified week: '{worksheet_to_add.title}' (Tab Index: {sog_tab})")
            else:
                print(f"Skipping specified worksheet '{worksheet_to_add.title}' (Tab Index: {sog_tab}) as it's a excluded sheet.")
        else:
            print(f"Error: Specified target tab index {sog_tab} is out of bounds for the number of worksheets available ({len(all_worksheets)}).")
            return
    else:
        sheets_to_process = [s.title for s in all_worksheets if s.title not in ["Welcome!", "Template"]]
        print("Processing all sheets except 'Welcome!' and 'Template'.")
    for sheet_name in sheets_to_process:
        worksheet = wks_SOG.worksheet(sheet_name)
        try:
            Organize_Sheet(worksheet, wks_SOG)
        except Exception as e:
            print(f"!!! An error occurred while processing sheet '{sheet_name}': {e}")
    print('\nAll sheets processed.')

def filldown_dates_in_sheet(worksheet, *, date_header_name: str = "Date") -> None:
    """
    1) Unmerge the Date column across all data rows.
    2) Forward-fill the Date cells in-sheet so every event row has a concrete date string
       (e.g., 'Monday, September 22') rather than blanks from merged cells.

    Assumptions:
      - Headers row is 3 (1-based); data starts at row 4.
      - A header named 'Date' (case-insensitive).
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    header_row_index = 2   # 0-based -> row 3 in the sheet
    data_start_row_idx = 3 # 0-based -> row 4 in the sheet

    # Pull grid
    all_vals = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if not all_vals or len(all_vals) <= header_row_index:
        return

    headers = list(all_vals[header_row_index])
    # trim trailing empties
    while headers and headers[-1] == "":
        headers.pop()
    if not headers:
        return

    # locate Date col
    date_col_idx = None
    for i, h in enumerate(headers):
        if str(h).strip().lower() == date_header_name.lower():
            date_col_idx = i
            break
    if date_col_idx is None:
        return

    # Unmerge the Date column across the data region (safe even if nothing is merged)
    try:
        worksheet.spreadsheet.batch_update({
            "requests": [{
                "unmergeCells": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": data_start_row_idx,         # 0-based
                        "endRowIndex": len(all_vals),                 # end-exclusive
                        "startColumnIndex": date_col_idx,
                        "endColumnIndex": date_col_idx + 1
                    }
                }
            }]
        })
    except Exception as e:
        print(f"[warn] unmerge Date col failed: {e}")

    # Build a DF for forward fill
    data_rows = all_vals[data_start_row_idx:]
    norm = [r[:len(headers)] + [""] * max(0, len(headers) - len(r)) for r in data_rows]
    import pandas as pd
    df = pd.DataFrame(norm, columns=headers)

    # Normalize existing date cell to a TEXT display (handles serials like 45923, '45924.0', etc.)
    def _as_display_text(x):
        if x is None or str(x).strip() == "":
            return np.nan
        s = str(x).strip()
        # numeric like 45923 or 45924.0 -> Excel epoch (1899-12-30)
        try:
            fv = float(s)
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=int(round(fv)))
            return dt.strftime('%A, %B %d')
        except ValueError:
            pass
        # try generic parse; if it yields a date, format as text
        try:
            dt = pd.to_datetime(s, errors='raise')
            return dt.strftime('%A, %B %d')
        except Exception:
            return s  # already a text label like 'Monday, September 22'

    col = df.iloc[:, date_col_idx].map(_as_display_text)
    col = col.ffill()  # forward-fill blanks

    # Write back only the Date column using USER_ENTERED so it stays as text
    from gspread.utils import rowcol_to_a1
    start_row_1b = data_start_row_idx + 1
    end_row_1b   = data_start_row_idx + len(df)
    a1_start = rowcol_to_a1(start_row_1b, date_col_idx + 1)
    a1_end   = rowcol_to_a1(end_row_1b, date_col_idx + 1)
    rng = f"{a1_start}:{a1_end}"
    values = [[("" if (v is np.nan or v is None or str(v) == "nan") else str(v))] for v in col.tolist()]
    if values:
        worksheet.update(rng, values, value_input_option='USER_ENTERED')

def unmerge_columns_in_data(worksheet, header_names=("Date", "Notes")) -> None:
    """
    Unmerge vertical merges for the specified header columns across the data region.
    Headers on row 3 (1-based). Data starts on row 4.
    """
    all_vals = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if not all_vals or len(all_vals) <= 2:
        return

    headers = list(all_vals[2])
    while headers and headers[-1] == "":
        headers.pop()
    if not headers:
        return

    # map names -> indices
    wanted_idx = []
    for name in header_names:
        for i, h in enumerate(headers):
            if str(h).strip().lower() == name.lower():
                wanted_idx.append(i)
                break

    if not wanted_idx:
        return

    reqs = []
    for idx in wanted_idx:
        reqs.append({
            "unmergeCells": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 3,                # data start (0-based)
                    "endRowIndex": len(all_vals),      # end-exclusive
                    "startColumnIndex": idx,
                    "endColumnIndex": idx + 1
                }
            }
        })
    if reqs:
        worksheet.spreadsheet.batch_update({"requests": reqs})
