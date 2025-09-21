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
from datetime import date, timedelta
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

async def post_events(bot, wks, week_number, IDCol, program, calendar, p):
    Titles, Leaders, Leaders_mask, Dates, Start_Times, End_Times, Locations, Locations_mask, Descriptions, Descriptions_mask, Categories, Event_IDs, Colors = p
    events = [] # grab the events from saved file.
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
                print(
                    f"Skipping event '{Titles[j]}' (row {j}) as end time is not after start time: Start={Start_Times[j]}, End={End_Times[j]}")
                continue
            
            process = None
            ## Add Event to Internal Database
            print(Event_IDs)
            if (Event_IDs[j] == '' or Event_IDs[j] == None):
                process = "Creation"
                # Make sure to add that event_id back to the spreadsheet
                
                # Generate a unique event_id
                event_id = str(uuid.uuid4()) # A better way to generate a unique ID
                wks.update_cell(j+4, IDCol+2, event_id)

                # create event object (as a dictionary)
                event = {
                    "title": Titles[j],
                    "date": Dates[j].isoformat(), 
                    "start_time": Start_Times[j].isoformat(),
                    "end_time": End_Times[j].isoformat(),
                    "week": week_number,
                    "description": f'<b>Description: </b>{description_val} \n \n<b>Led by: </b>{leader_val} \n \n<b>Category: </b>{category_val}',
                    "location": Locations[j],
                    "leaders": Leaders[j],
                    "category": Categories[j],
                    "recording": None,
                    "id": event_id,
                    "discord_id": 0,
                    "calendar_id": 0,
                    "status": "Active"
                }
                
                events.append(event)
            else:
                print(f"Event ID :{Event_IDs[j]}")
                process = "Update"
                event = None
                for event_j in events:
                    if (Event_IDs[j] == event_j["id"]):
                        event = event_j
                        break # Found the event, no need to keep searching

                if event:
                    # Update internal memory of event
                    event["title"] = Titles[j]
                    event["date"] = Dates[j].isoformat()
                    event["start_time"] = Start_Times[j].isoformat()
                    event["end_time"] = End_Times[j].isoformat()
                    event["status"] = "Active"
                else:
                    print(f"Error: Could not find event with ID {Event_IDs[j]} to update.")
                
            gc_event = Event(
                    Titles[j],
                    start=Start_Times[j],
                    end=End_Times[j],
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

                    discord_id = await update_or_create_discord_event(bot, program,
                        Titles[j],
                        description_val,
                        Start_Times[j],
                        End_Times[j],
                        location_val,
                    )

                    event["calendar_id"] = calendar_id
                    event["discord_id"] = discord_id
                else:
                    print(
                        f'Google Calendar event not posted: {Titles[j]} (Start Time: {Start_Times[j]}) since event time has passed.')
            elif(process == "Update"):
                if(event["status"] == "Active"):
                    # Step 1: Get the existing event object from Google Calendar using its ID
                    # The calendar.get_event method is used to retrieve a single event by its ID
                    gc_event = calendar.get_event(event_id=event["calendar_id"])

                    # Step 2: Update the attributes of the existing event object
                    gc_event.summary = Titles[j] # Use .summary for the title
                    gc_event.start = Start_Times[j]
                    gc_event.end = End_Times[j]
                    gc_event.location = location_val
                    gc_event.description = f'<b>Description: </b>{description_val} \n \n<b>Led by: </b>{leader_val} \n \n<b>Category: </b>{category_val}'
                    gc_event.color_id = Colors[j] if j < len(Colors) else Missing_color
                    gc_event.minutes_before_popup_reminder = 30
                    # Make sure to set the event_id as well
                    gc_event.event_id = event["calendar_id"]
                else:
                    calendar.delete_event(event["calendar_id"])
                    
                discord_id = await update_or_create_discord_event(bot, program,
                    Titles[j],
                    description_val,
                    Start_Times[j],
                    End_Times[j],
                    location_val,
                    event["discord_id"],
                    event["status"]
                )
        
        else:
            print(
                f"Skipping row {j} due to missing data: Date={Dates[j]}, Start_Time={Start_Times[j]}, End_Time={End_Times[j]}, Title={Titles[j]}")
            
    # Save the updated events list
    with open(EVENT_DATA_FILE, 'w') as f:
        json.dump(events, f, indent=4)

async def update_events(bot, wks, program, calendar, event_ID, update_args = None):
    events = [] # grab the events from saved file.
    try:
        with open(EVENT_DATA_FILE, 'r') as f:
            events = json.load(f)
    except FileNotFoundError:
        print("Event data file not found. Starting with an empty list.")
    
    event0 = None
    for event in events:
        if(event["id"] == event_ID):
            event0 = event
            print("Found Event to Update")
            break
    if(event0 is not None):

async def update_events(bot, wks, program, calendar, event_ID, update_args = None):
    events = [] # grab the events from saved file.
    try:
        with open(EVENT_DATA_FILE, 'r') as f:
            events = json.load(f)
    except FileNotFoundError:
        print("Event data file not found. Starting with an empty list.")
    
    event0 = None
    for event in events:
        if(event["id"] == event_ID):
            event0 = event
            print("Found Event to Update")
            break
    
    if(event0 is not None):
        if update_args is None:
            # If no update arguments are provided, do nothing.
            print("No updates provided, aborting.")
            return

        # Update each field safely by checking if it exists and is not None
        if update_args.get("title") is not None:
            event0["title"] = update_args["title"]
        
        if update_args.get("date") is not None:
            # Re-parse the date string into a datetime object
            event0["date"] = datetime.datetime.fromisoformat(update_args["date"]).date()
        
        if update_args.get("start_time") is not None:
            # Re-parse the time string into a datetime.time object
            event0["start_time"] = datetime.datetime.strptime(update_args["start_time"], "%I:%M %p").time()
        
        if update_args.get("end_time") is not None:
            # Re-parse the time string into a datetime.time object
            event0["end_time"] = datetime.datetime.strptime(update_args["end_time"], "%I:%M %p").time()
        
        if update_args.get("leaders") is not None:
            event0["leaders"] = update_args["leaders"]
            
        if update_args.get("location") is not None:
            event0["location"] = update_args["location"]
            
        if update_args.get("category") is not None:
            event0["category"] = update_args["category"]
            
        if update_args.get("description") is not None:
            event0["description"] = update_args["description"]
            
        if update_args.get("recording") is not None:
            event0["recording"] = update_args["recording"]
            
        if update_args.get("status") is not None:
            event0["status"] = update_args["status"]
            
        print("Successfully Updated Internal Memory of Event")
        
        # Determine the week number from the event's data
        week_number = int(event0.get("week", 0))

        start_time_date = None
        end_time_date = None
        
        # Prepare start and end datetime objects for API calls
        if event0.get("date") and event0.get("start_time"):
            start_time_date = eastern.localize(datetime.datetime.combine(event0["date"], event0["start_time"]))
        if event0.get("date") and event0.get("end_time"):
            end_time_date = eastern.localize(datetime.datetime.combine(event0["date"], event0["end_time"]))
            
        week_number = int(event0["week"])
        print("Successfully Updated Internal Memory of Event")

        start_time_date = parse_times([event0["date"]], [event0["start_time"]])[0]
        end_time_date = parse_times([event0["date"]], [event0["end_time"]])[0]
        
        ### --- Update Google Calendar --- ###
        if(event0["status"] == "Active"):
            # Step 1: Get the existing event object from Google Calendar using its ID
            gc_event = calendar.get_event(event_id=event0["calendar_id"])

            # Step 2: Update the attributes of the existing event object
            gc_event.summary = event0["title"] # Use .summary for the title
            gc_event.start = event0["start_time"]
            gc_event.end = event0["end_time"]
            gc_event.location = event0["location"]
            gc_event.description = f'<b>Description: </b>{event0["description"] } \n \n<b>Led by: </b>{event0["leaders"]} \n \n<b>Category: </b>{event0["category"]}'
            # gc_event.color_id = Colors[j] if j < len(Colors) else Missing_color ### implement colors later
            gc_event.minutes_before_popup_reminder = 30
        else:
            calendar.delete_event(event0["calendar_id"])

        ### --- Update Discord --- ###
        await update_or_create_discord_event(bot, program,
            event0["title"],
            f'<b>Description: </b>{event0["description"] } \n \n<b>Led by: </b>{event0["leaders"]} \n \n<b>Category: </b>{event0["category"]}',
            start_time_date,
            end_time_date,
            event0["location"],
            event0["discord_id"],
            event0["status"]
        )
        
        ### -- Update SOG --- ### 
        # TODO: Get the SOG & find the event (matching to UUID)
        Verbose_Sheet(program, wks, week_number)
        
        SOG_WKS = pd.DataFrame(wks.get_worksheet(week_number+2).get_all_values(value_render_option='UNFORMATTED_VALUE'))[2:][:]
        headers = SOG_WKS.iloc[0].values
        SOG_WKS.columns = headers
        SOG_WKS = SOG_WKS[1:]
    
        Dates = SOG_WKS['Date'].tolist()
        Titles = SOG_WKS['Workshop Title'].tolist()

        ### Datetime objects are in Excel format & need to be converted.
        for j in range(0, len(Dates)):
            if isinstance(Dates[j], (int, float)):
                Dates[j] = conversion_excel_date(Dates[j])

        ### Determine ranges for specific dates from the SOG (as it goes Date .... Date.... Date....)
        last_valid_date = None
        for j in range(len(Dates)):
            if isinstance(Dates[j], datetime.datetime):
                last_valid_date = Dates[j]
            elif Dates[j] == '' and last_valid_date is not None:
                Dates[j] = last_valid_date
            else:
                Dates[j] = None

        # Leaders = SOG_WKS['Led By'].tolist()
        # Descriptions = SOG_WKS['Description'].tolist()
        # Locations = SOG_WKS['Location/Link'].tolist()
        # Categories = SOG_WKS['Category'].tolist()
        
        # Start_Times = parse_times(Dates, SOG_WKS['Start Time'].tolist())
        # End_Times = parse_times(Dates, SOG_WKS['End Time'].tolist())
        Event_IDs = SOG_WKS['Event ID'].tolist()        
    
        ii = None
        for (i,event_id) in enumerate(Event_IDs):
            if(event_id == event0["id"]):
                ii = i
                
        row_offset = 4
        if(ii != None):
            # Found Matching Event in SOG. Updating... 
            wks.update_cell(ii+row_offset, 0, event0["date"])
            wks.update_cell(ii+row_offset, 1, "Updated Details!")
            wks.update_cell(ii+row_offset, 2, event0["title"])
            wks.update_cell(ii+row_offset, 3, event0["leaders"])
            wks.update_cell(ii+row_offset, 4, event0["start_time"])
            wks.update_cell(ii+row_offset, 5, event0["end_time"])
            wks.update_cell(ii+row_offset, 6, event0["description"])
            wks.update_cell(ii+row_offset, 7, event0["location"])
            wks.update_cell(ii+row_offset, 9, event0["category"])
            wks.update_cell(ii+row_offset, 10, event0["recording"])        
        Reorganize_Sheet(program, wks, week_number)
        return             
            
    # Save the updated events list
    with open(EVENT_DATA_FILE, 'w') as f:
        json.dump(events, f, indent=4)


### Org Prog Jawn


def prog_weeks(Weeks_arr):
    ii_w = []
    i0 = 0
    i1 = 0
    for i in range(0, len(Weeks_arr)):
        
        if(i == 0):
            i0 = i
        elif(Weeks_arr[i] != '' and not (isinstance(Weeks_arr[i],int))):
            # print(f'Weeks_arr[i] = {Weeks_arr[i]}')
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
    """
    Cleans a list of headers by:
    1. Stripping whitespace.
    2. Replacing empty strings with a unique placeholder (e.g., 'Unnamed 0').
    3. Handling duplicate names by appending a counter (e.g., 'Date_1').
    """
    cleaned = []
    seen_headers = {}
    for i, h in enumerate(raw_headers_list):
        # Convert non-string headers to string for consistency if needed,
        # but for column names, they usually come in as strings or numbers that should be strings.
        header_str = str(h).strip()

        if not header_str: # If header is empty after stripping
            header_str = f"{prefix}_{i}" # Use a unique unnamed placeholder

        original_header_str = header_str
        count = seen_headers.get(original_header_str, 0)
        if count > 0:
            header_str = f"{original_header_str}_{count}"
        seen_headers[original_header_str] = count + 1 # Increment for the next potential duplicate

        cleaned.append(header_str)
    return cleaned

### FSI Programming

# Your global variables (if any) and initial setup remain the same
row_print_offset = 4 

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
    ## Returns indice ranges for different weeks of the programming
    ii_w = prog_weeks(Weeks_arr)

    print(f'Printing events for Week #{week_number} from Programming sheet #{PROGRAMMING}...')

    # if(PROGRAMMING == 1): # Because programming typically occurs on the second week of Online
    #     Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr = get_programming(cal_data, ii_w[week_number])
    # else:
    #     Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr = get_programming(cal_data, ii_w[week_number-1])
    Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr = get_programming(cal_data, ii_w[week_number-1])

    ##### OUTPUT SPREADSHEET (SOG)
    worksheet_SOG_index = 2 + week_number
    worksheet_SOG = wks_SOG.get_worksheet(worksheet_SOG_index)
    
    # Define the row where headers are in SOG (0-indexed)
    sog_header_row_gspread_idx = 2 # Row 3 in Google Sheet
    sog_data_start_row_gspread_idx = sog_header_row_gspread_idx + 1 # Row 4 in Google Sheet

    # --- Store columns M and N before any modifications ---
    full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
    headers_SOG_raw = full_sog_values[sog_header_row_gspread_idx]
    
    ### REVIEW THIS SECTION FOR THOSE WITHOUT RECORDING

    ### Columns N (12) & M (13) are for the HALPS category. Pad and overwrite at the end.
    while len(headers_SOG_raw) <= 13: ######### 
        headers_SOG_raw.append('') 

    stored_column_M_values = [row[12] if len(row) > 12 else '' for row in full_sog_values[sog_data_start_row_gspread_idx:]]
    stored_column_N_values = [row[13] if len(row) > 13 else '' for row in full_sog_values[sog_data_start_row_gspread_idx:]]
    
    print(f"Stored {len(stored_column_M_values)} values for column M.")
    print(f"Stored {len(stored_column_N_values)} values for column N.")

    cal_data_SOG = pd.DataFrame(worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE'))[sog_header_row_gspread_idx:][:]
    cal_data_SOG.columns = cal_data_SOG.iloc[0].values # Use the first row of this slice as headers
    cal_data_SOG = cal_data_SOG[1:].reset_index(drop=True) # Remove header row from data
    current_df_headers_SOG = cal_data_SOG.columns.tolist()
    
    # Ensure the header for the date column (index 0) exists
    date_col_header = current_df_headers_SOG[0] if len(current_df_headers_SOG) > 0 else 'Column1' 
    Dates_arr_SOG = cal_data_SOG[date_col_header]
    ii_d_SOG = sog_days(Dates_arr_SOG)

    # Loop through each event from the source "Programming" sheet for the current week
    for j in range(len(Date_arr)):
        current_input_date = Date_arr[j]
        current_input_name = Name_arr[j]

        # Find the date block in the SOG that matches the current event's date
        sog_day_block_index = -1
        for k in range(len(ii_d_SOG)):
            first_row_of_block_df_idx = ii_d_SOG[k][0]
            
            # Use .get with a default to avoid KeyError if column doesn't exist
            name_col_header = current_df_headers_SOG[2] if len(current_df_headers_SOG) > 2 else 'Column3'
            
            # Ensure index is within bounds before accessing
            if first_row_of_block_df_idx < len(Dates_arr_SOG) and Dates_arr_SOG.iloc[first_row_of_block_df_idx] == current_input_date:
                sog_day_block_index = k
                break
        
        if sog_day_block_index == -1:
            print(f"Warning: Date {current_input_date} not found in SOG sheet index {worksheet_SOG_index}. Skipping event '{current_input_name}'.")
            continue

        # Get the range of rows for this day's block in the SOG DataFrame
        day_start_df_idx, day_end_df_idx = ii_d_SOG[sog_day_block_index]
        
        # Re-read Name_arr_SOG from the current state of cal_data_SOG
        # This is important if previous insertions in the same day block modified row indices
        Name_arr_SOG = cal_data_SOG[name_col_header][day_start_df_idx : day_end_df_idx + 1].reset_index(drop=True)

        ######### -- FIX MATCHING CRITERIAA -- #########
        # Check if the event already exists in that day's block by name 
        match_found_at_sog_df_index = -1
        for l in range(len(Name_arr_SOG)):
            if Name_arr_SOG[l] == current_input_name:
                match_found_at_sog_df_index = l
                break
        
        # Prepare the data payload for the row to be written to Google Sheet
        new_row_data = [
            current_input_name, Host_arr[j], Start_arr[j], End_arr[j], 
            Description_arr[j], Location_arr[j], 1, HALPS_arr[j]
        ]
    
        if match_found_at_sog_df_index != -1:
            # --- ACTION 1: UPDATE EXISTING EVENT ---
            print(f"Updating Event: '{current_input_name}'")
            
            # Calculate the exact sheet row to update
            # DataFrame index + (header row index + 1 for 1-based indexing)
            update_row_sheet = day_start_df_idx + match_found_at_sog_df_index + sog_data_start_row_gspread_idx +1
            
            print(f"  -> Found at SOG DataFrame index: {day_start_df_idx + match_found_at_sog_df_index}, Updating Sheet row: {update_row_sheet}")
            
            # Directly update the specific range in the Google Sheet
            range_for_row = f"C{update_row_sheet}:J{update_row_sheet}"
            try:
                worksheet_SOG.update(range_for_row, [new_row_data])
                print(f"  -> Successfully updated event '{current_input_name}' in sheet.")
            except APIError as e:
                print(f"  -> Error updating row {update_row_sheet} for event '{current_input_name}': {e.response.text}")

        else:
            # --- ACTION 2: CREATE NEW EVENT ---
            print(f"Creating Event: '{current_input_name}'")
            
            # Sheet row to insert AT = last row of the block (in df) + offset of actual data start + 1 (for insertion point)
            insert_row_sheet = day_end_df_idx + sog_data_start_row_gspread_idx + 1 +1 # +1 to insert *after* the current block
                                                                                        # This makes it day_end_df_idx + sog_data_start_row_gspread_idx + 2

            print(f"  -> Inserting data at Sheet row: {insert_row_sheet}")
            
            # The data to insert needs empty columns for 'Date' and the second column (A and B)
            # followed by the actual event data (C to J)
            insert_row_data_full = ['', ''] + new_row_data

            try:
                worksheet_SOG.insert_row(insert_row_data_full, index=insert_row_sheet)
                print(f"  -> Successfully inserted new event '{current_input_name}'.")
            except APIError as e:
                print(f"  -> Error inserting row at {insert_row_sheet} for event '{current_input_name}': {e.response.text}")

            # *** NEW LOGIC FOR COPYING STYLE ***
            worksheet_id = worksheet_SOG.id

            source_row_1_indexed = insert_row_sheet - 1 # Row above the newly inserted one
            destination_row_1_indexed = insert_row_sheet

            source_start_row_api = source_row_1_indexed - 1
            source_end_row_api = source_row_1_indexed 

            destination_start_row_api = destination_row_1_indexed - 1
            destination_end_row_api = destination_row_1_indexed 

            copy_up_to_column_exclusive_index = 10 # Copies columns A (0) up to J (9)

            requests = [{
                "copyPaste": {
                    "source": {
                        "sheetId": worksheet_id,
                        "startRowIndex": source_start_row_api,
                        "endRowIndex": source_end_row_api,
                        "startColumnIndex": 0,
                        "endColumnIndex": copy_up_to_column_exclusive_index
                    },
                    "destination": {
                        "sheetId": worksheet_id,
                        "startRowIndex": destination_start_row_api,
                        "endRowIndex": destination_end_row_api,
                        "startColumnIndex": 0,
                        "endColumnIndex": copy_up_to_column_exclusive_index
                    },
                    "pasteOrientation": "HORIZONTAL",
                    "pasteType": "PASTE_FORMAT" 
                }
            }]
            
            try:
                wks_SOG.batch_update({"requests": requests})
                print(f"  -> Successfully sent request to copy style.")
            except APIError as e:
                print(f"  -> Error copying style for row {destination_row_1_indexed}: {e.response.text}")
            # *** END NEW LOGIC ***

            # After an insert, we MUST re-read the entire SOG sheet for the current week
            # to get the accurate DataFrame and ii_d_SOG indices. This is CRITICAL.
            print("  -> Re-reading SOG data after insertion to refresh in-memory DataFrame and indices.")
            
            full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
            
            # Re-parse headers and data
            # headers_SOG_raw (the one defined at the top of the week loop) should still be valid for column names
            cal_data_SOG = pd.DataFrame(full_sog_values[sog_header_row_gspread_idx:][:], columns=headers_SOG_raw)
            cal_data_SOG = cal_data_SOG[1:].reset_index(drop=True)
            
            current_df_headers_SOG = cal_data_SOG.columns.tolist() # Update headers from re-read
            date_col_header = current_df_headers_SOG[0] # Should be robust now
            Dates_arr_SOG = cal_data_SOG[date_col_header] # Re-fetch based on updated cal_data_SOG

            # Re-calculate ii_d_SOG based on the freshly read data
            ii_d_SOG = [] 
            i0_re = 0
            i1_re = 0
            for k_re in range(0, len(Dates_arr_SOG)):
                if (k_re == 0):
                    i0_re = k_re
                elif (Dates_arr_SOG[k_re] != ''):
                    if(k_re == i0_re+1 and Dates_arr_SOG[i0_re] != ''):
                        ii_d_SOG.append([i0_re, i0_re])
                    elif (Dates_arr_SOG[k_re] == 'Ongoing Challenges'):
                        ii_d_SOG.append([i0_re, i1_re-1])
                    else:
                        ii_d_SOG.append([i0_re, i1_re])
                    i0_re = k_re
                else:
                    i1_re = k_re
            if i0_re <= i1_re:
                ii_d_SOG.append([i0_re, i1_re])
            print(f"  -> ii_d_SOG re-calculated for sheet {worksheet_SOG_index}: {ii_d_SOG}")


    # --- End of week loop: Re-paste columns M and N ---
    # First, get the current number of data rows in the sheet after all modifications
    # We need to re-read the sheet one last time to get the most accurate current state for all rows.
    final_full_sog_values = worksheet_SOG.get_all_values(value_render_option='UNFORMATTED_VALUE')
    final_data_rows_count = len(final_full_sog_values) - sog_data_start_row_gspread_idx 

    # Pad stored_column_M_values and stored_column_N_values to match the current number of data rows
    if stored_column_M_values is not None:
        if len(stored_column_M_values) < final_data_rows_count:
            stored_column_M_values.extend([''] * (final_data_rows_count - len(stored_column_M_values)))
        elif len(stored_column_M_values) > final_data_rows_count:
            stored_column_M_values = stored_column_M_values[:final_data_rows_count]
    else:
        stored_column_M_values = [''] * final_data_rows_count # Fallback if M was never stored

    if stored_column_N_values is not None:
        if len(stored_column_N_values) < final_data_rows_count:
            stored_column_N_values.extend([''] * (final_data_rows_count - len(stored_column_N_values)))
        elif len(stored_column_N_values) > final_data_rows_count:
            stored_column_N_values = stored_column_N_values[:final_data_rows_count]
    else:
        stored_column_N_values = [''] * final_data_rows_count # Fallback if N was never stored

    # Prepare data for updating columns M and N
    # We need to create a list of lists, where each inner list is a single row's M and N values
    # gspread update expects [ [M1, N1], [M2, N2], ... ] for a range like M:N
    
    # We need to skip the header row when constructing this.
    update_range_start_row = sog_data_start_row_gspread_idx + 1 # 1-indexed for gspread update
    
    # Prepare data in the correct format for the update
    m_n_data_for_update = []
    for row_idx in range(final_data_rows_count):
        m_n_data_for_update.append([stored_column_M_values[row_idx], stored_column_N_values[row_idx]])

    # Update columns M and N directly on the sheet
    if m_n_data_for_update: # Only update if there's data
        try:
            # Update range will be from column M to N, starting from where data begins
            range_m_n_update = f"M{update_range_start_row}:N{update_range_start_row + final_data_rows_count - 1}"
            worksheet_SOG.update(range_m_n_update, m_n_data_for_update)
            print(f"Successfully re-pasted columns M and N for sheet '{worksheet_SOG.title}'.")
        except APIError as e:
            print(f"Error re-pasting columns M and N for sheet '{worksheet_SOG.title}': {e.response.text}")
    else:
        print(f"No data to re-paste for columns M and N in sheet '{worksheet_SOG.title}'.")

    print('Printing completed.')

def Deduplicate_Headers(headers):
    """Ensures all column headers are unique by appending suffixes to duplicates."""
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
    """
    Parses date strings like "Monday, July 7" from a cell, returning datetime objects.
    Falls back to the numeric date if string parsing fails.
    """
    found_strings = re.findall(r'(\w+,\s\w+\s\d+)', str(cell_value))
    if found_strings:
        return [pd.to_datetime(d, errors='coerce') for d in found_strings]
    
    if pd.notna(numeric_date):
        try:
            origin = pd.Timestamp('1899-12-30')
            return [origin + pd.to_timedelta(float(numeric_date), unit='D')]
        except (ValueError, TypeError):
            return []
    return []

def Format_Time(numeric_time):
    """Converts a spreadsheet numeric time to a 12-hour string (e.g., "5:00pm")."""
    if pd.isna(numeric_time): return ""
    try:
        total_seconds = int(float(numeric_time) * 86400)
    except (ValueError, TypeError):
        return ""
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours >= 24: hours, minutes = 23, 59
    try:
        t = time(hour=hours, minute=minutes)
        if t.minute == 0:
            return t.strftime('%-I%p').lower()
        else:
            return t.strftime('%-I:%M%p').lower()
    except ValueError:
        return ""

def Organize_Sheet(worksheet, spreadsheet_obj):
    """
    Main logic to read, process, sort, and write back data for a single sheet.
    Assumes invariant sheet structure (no column addition/removal in terms of final output width),
    and that 'Led By' content remains in 'Led By' column, 'Notes' remains in 'Notes'.
    """
    print(f"--- Processing sheet: '{worksheet.title}' ---")
    
    all_sheet_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    
    header_row_index = 2 
    data_start_row_index = 3 

    if len(all_sheet_data) <= header_row_index:
        print(f"Skipping sheet '{worksheet.title}': Not enough rows to find headers.")
        return
    
    # --- Store original columns M and N before processing ---
    # These correspond to indices 12 and 13 (0-indexed)
    # We need to ensure that the all_sheet_data has at least 14 columns
    # and that the rows are long enough.
    
    # Pad rows to ensure they have enough columns to extract M and N
    padded_all_sheet_data = []
    for row in all_sheet_data:
        # Ensure row has at least enough columns to access up to N (index 13)
        padded_row = row + [''] * (14 - len(row)) if len(row) < 14 else row
        padded_all_sheet_data.append(padded_row)

    # Store M and N data from the padded data.
    # Note: These are for the *data rows only* (from data_start_row_index onwards)
    original_col_M_data = [row[12] for row in padded_all_sheet_data[data_start_row_index:]]
    original_col_N_data = [row[13] for row in padded_all_sheet_data[data_start_row_index:]]
    
    # --- Original DataFrame creation: Process ALL meaningful columns ---
    # We are NOT excluding M and N from the initial DataFrame here.
    # The DataFrame 'df' will contain ALL columns that have headers.
    actual_meaningful_headers_raw = [h for h in all_sheet_data[header_row_index] if str(h).strip() != '']
    print(f"DEBUG: actual_meaningful_headers_raw: {actual_meaningful_headers_raw}")
    
    initial_df_cols = Deduplicate_Headers(actual_meaningful_headers_raw)
    print(f"DEBUG: initial_df_cols after deduplication: {initial_df_cols}")
    
    num_meaningful_cols = len(initial_df_cols)

    if len(all_sheet_data) <= data_start_row_index:
        print(f"Skipping sheet '{worksheet.title}': No data rows to process.")
        return

    raw_data_for_df = all_sheet_data[data_start_row_index:] 
    
    padded_data = [
        (row + [''] * (num_meaningful_cols - len(row)))[:num_meaningful_cols] 
        for row in raw_data_for_df
    ]
    
    df = pd.DataFrame(padded_data, columns=initial_df_cols)
    df = df.replace('', np.nan).infer_objects(copy=False) 

    print("\n--- DEBUG: DataFrame 'df' after initial creation (ALL columns) ---")
    print(df.head(10)) 
    print(df.columns) 
    print("-----------------------------------------------------------\n")

    # --- Column Name Constants (using direct names from `initial_df_cols`) ---
    date_col_name = 'Date'
    notes_col_name = 'Notes'
    title_col_name = 'Workshop Title'
    led_by_col_name = 'Led By'
    start_time_col_name = 'Start Time'
    end_time_col_name = 'End Time'
    description_col_name = 'Description'
    location_col_name = 'Location/Link'
    halps_col_name = 'HALPS Points'
    category_col_name = 'Category'
    recording_col_name = 'Recording'
    
    # This will now include M and N if they have headers
    FINAL_DISPLAY_HEADERS = initial_df_cols 

    # Check for essential columns for processing. M and N are not essential for *this* processing logic.
    essential_cols = [date_col_name, notes_col_name, title_col_name, start_time_col_name]
    if not all(col in df.columns for col in essential_cols):
        print(f"Skipping sheet '{worksheet.title}': Essential columns missing after header processing.")
        print(f"Expected: {essential_cols}, Found: {df.columns.tolist()}")
        return
    
    time_col = start_time_col_name 

    print(f"DEBUG: df.columns (after all initial setup, before split): {df.columns}")

    split_idx = df.index[df[date_col_name] == 'Ongoing Challenges'].min() if 'Ongoing Challenges' in df[date_col_name].values else len(df)
    
    events_df_raw = df.iloc[:split_idx].copy()
    ongoing_df = df.iloc[split_idx:].copy()

    # --- Get Week Start Date from Sheet Title ---
    week_start_date = pd.NaT
    sheet_title = worksheet.title
    
    week_date_match = re.search(r'\((\w+\s\d+)-\d+\)', sheet_title)
    
    year_match = re.search(r"(?:FSI'|20)(\d{2})", sheet_title, re.IGNORECASE) 
    if not year_match and spreadsheet_obj: 
        year_match = re.search(r"(?:FSI'|20)(\d{2})", spreadsheet_obj.title, re.IGNORECASE)
    
    print(f"DEBUG: Sheet title: '{sheet_title}'")
    if spreadsheet_obj:
        print(f"DEBUG: Spreadsheet title: '{spreadsheet_obj.title}'")
    print(f"DEBUG: year_match object found: {year_match}") 

    year_found = pd.Timestamp.now().year 
    if year_match and len(year_match.groups()) >= 1: 
        year_suffix = year_match.group(1) 
        year_found = int(f"20{year_suffix}")
    else:
        print(f"DEBUG ERROR: No year match found or year group is empty. Defaulting to {year_found}.")

    # --- Date Assignment Logic (for each event row) ---
    events_df_raw['Assigned_Date'] = pd.NaT 
    last_valid_assigned_date = pd.NaT

    for i in events_df_raw.index: 
        date_cell_value = events_df_raw.loc[i, date_col_name]
        parsed_numeric_val = pd.to_numeric(date_cell_value, errors='coerce')

        assigned_current_date = pd.NaT 
        
        if pd.notna(parsed_numeric_val):
            if parsed_numeric_val > 1000: 
                parsed_dates = Parse_Dates(str(date_cell_value), parsed_numeric_val)
                if parsed_dates and pd.notna(parsed_dates[0]):
                    assigned_current_date = parsed_dates[0]
            elif parsed_numeric_val >= 1 and pd.notna(week_start_date): 
                assigned_current_date = week_start_date + pd.Timedelta(days=int(parsed_numeric_val) - 1)
        
        if pd.notna(assigned_current_date):
            events_df_raw.loc[i, 'Assigned_Date'] = assigned_current_date
            last_valid_assigned_date = assigned_current_date
        else:
            events_df_raw.loc[i, 'Assigned_Date'] = last_valid_assigned_date
    
    events_df_raw['Assigned_Date'] = events_df_raw['Assigned_Date'].ffill() 

    # Filter `events_df` from `events_df_raw` based on essential content
    events_df = events_df_raw.dropna(subset=[title_col_name, start_time_col_name], how='all').copy()
    events_df = events_df.dropna(subset=['Assigned_Date']).copy() 
    events_df.reset_index(drop=True, inplace=True)
    
    print("\n--- DEBUG: events_df after date assignment and filtering ---")
    print(events_df.head(10))
    print(events_df.columns)
    print("-----------------------------------------------------------\n")

    # --- Sort the processed events ---
    # ADDED: Store the original Start Time string before converting to a numeric for sorting.
    events_df['Original_Start_Time'] = events_df[start_time_col_name].copy()
    
    events_df['numeric_start_time'] = pd.to_numeric(events_df[start_time_col_name], errors='coerce')
    events_df['sort_key'] = events_df['Assigned_Date'] + pd.to_timedelta(events_df['numeric_start_time'], unit='D', errors='coerce')
    events_df.sort_values(by=['sort_key'], inplace=True, na_position='last')

    # --- Format Date and Time Columns for Output ---
    events_df[date_col_name] = events_df['Assigned_Date'].dt.strftime('%A, %B %d')
    
    # MODIFIED: Re-assign the original time strings instead of re-formatting from the numeric value.
    # This prevents the original time strings from being lost.
    # events_df[start_time_col_name] = events_df['numeric_start_time'].apply(Format_Time)
    events_df[start_time_col_name] = events_df['Original_Start_Time']

    # MODIFIED: Drop the new temporary column along with the others.
    events_df.drop(columns=['Assigned_Date', 'numeric_start_time', 'sort_key', 'Original_Start_Time'], inplace=True)

    # --- Recombine and Write Back ---
    
    # Create the base processed DataFrame including all columns from original df
    # but with sorting applied to event rows.
    # The 'ongoing_df' already maintains its original columns.
    
    # Reindex events_df to ensure it has all the original columns (including M and N if they exist in initial_df_cols)
    processed_events_df = events_df.reindex(columns=FINAL_DISPLAY_HEADERS, fill_value='')

    # Create a blank row with the same columns as the final DataFrame
    blank_row_df = pd.DataFrame([[''] * len(FINAL_DISPLAY_HEADERS)], columns=FINAL_DISPLAY_HEADERS)

    # Concatenate the parts
    final_df = pd.concat([processed_events_df, blank_row_df, ongoing_df], ignore_index=True)

    # Re-align M and N data *only for the rows that were NOT part of the events_df*
    # because events_df's M and N might have been reordered.
    # This is the tricky part to get right:
    # 1. For the `processed_events_df` section: Use the M and N from the sorted `events_df`.
    # 2. For the `blank_row_df` and `ongoing_df` sections: Use the `original_col_M_data` and `original_col_N_data`

    # Let's rebuild the final data explicitly row by row, or use a more robust DataFrame merge/assign.
    # The current `final_df` already contains all columns, *including* M and N from the processed `events_df`
    # and the original `ongoing_df`.
    # The only place M and N might have changed is if they were part of the 'events_df' and got reordered.
    # If the requirement is "don't modify M and N AT ALL, even if they are in rows that get sorted",
    # then we need a different approach.

    # Let's redefine the strategy:
    # 1. Read all sheet data.
    # 2. Store M and N columns *separately* for the entire data range that gets re-written (rows 4 onwards).
    # 3. Create a DataFrame *excluding* M and N for processing.
    # 4. Process that DataFrame.
    # 5. Concatenate the processed parts.
    # 6. Re-insert M and N into the final list of lists *just before writing back*.

    # This is the strategy from my previous response, but without the "excluding from initial df" step.
    # I believe the previous response was correct in its strategy (exclude, process, re-insert)
    # but the implementation of which columns to exclude/include caused the shift.

    # Let's adjust back to the "exclude from DataFrame processing" but ensure
    # the correct columns are handled.

    # Reverting to the strategy of creating 'df' *without* M & N for core processing.
    # The issue was I adjusted `initial_df_cols` to be only A-L.
    # Instead, we should create a dataframe with A-L, and keep the original full `initial_df_cols`
    # for the `FINAL_DISPLAY_HEADERS`.

    # Let's re-implement the M and N preservation more carefully:

    # 1. Read ALL data and headers
    all_sheet_data_full = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')

    # Ensure all rows in full data have enough columns for M and N
    padded_all_sheet_data_full = []
    for row in all_sheet_data_full:
        padded_row = row + [''] * (max(14, len(all_sheet_data_full[header_row_index]) ) - len(row)) # Pad to the max of 14 or header row length
        padded_all_sheet_data_full.append(padded_row)


    # Extract headers for all columns
    full_original_headers_raw = padded_all_sheet_data_full[header_row_index]
    full_deduplicated_headers = Deduplicate_Headers(full_original_headers_raw)

    # Store M and N columns for the *entire data region that will be rewritten*
    # This means from data_start_row_index until the end of the sheet's current content.
    # We need to preserve original length.
    stored_col_M_values = [row[12] for row in padded_all_sheet_data_full[data_start_row_index:]]
    stored_col_N_values = [row[13] for row in padded_all_sheet_data_full[data_start_row_index:]]
    
    # Create a DataFrame *excluding* M and N columns for processing
    # The data for this DataFrame will be `all_sheet_data_full` sliced.
    # The headers for this DataFrame will be `full_deduplicated_headers` sliced.
    
    # Get the headers for columns A-L
    processed_df_headers = full_deduplicated_headers[:12] # Headers for A-L
    
    # Get the raw data for columns A-L
    processed_raw_data = [row[:12] for row in padded_all_sheet_data_full[data_start_row_index:]]

    # Create the DataFrame that will undergo sorting and modifications
    df = pd.DataFrame(processed_raw_data, columns=processed_df_headers)
    df = df.replace('', np.nan).infer_objects(copy=False)

    print("\n--- DEBUG: DataFrame 'df' after initial creation (ONLY A-L columns processed) ---")
    print(df.head(10)) 
    print(df.columns) 
    print("-----------------------------------------------------------\n")

    # ... (rest of your existing processing logic on 'df' remains the same) ...
    # This part should be identical to your original code since it operates on `df`
    # which now only contains A-L.

    date_col_name = 'Date'
    notes_col_name = 'Notes'
    title_col_name = 'Workshop Title'
    led_by_col_name = 'Led By'
    start_time_col_name = 'Start Time'
    end_time_col_name = 'End Time'
    description_col_name = 'Description'
    location_col_name = 'Location/Link'
    halps_col_name = 'HALPS Points'
    category_col_name = 'Category'
    recording_col_name = 'Recording'

    if not all(col in df.columns for col in [date_col_name, notes_col_name, title_col_name, start_time_col_name]):
        print(f"Skipping sheet '{worksheet.title}': Essential columns missing after header processing.")
        print(f"Expected: ['Date', 'Notes', 'Workshop Title', 'Start Time'], Found: {df.columns.tolist()}")
        return
    
    time_col = start_time_col_name

    split_idx = df.index[df[date_col_name] == 'Ongoing Challenges'].min() if 'Ongoing Challenges' in df[date_col_name].values else len(df)
    
    events_df_raw = df.iloc[:split_idx].copy()
    ongoing_df = df.iloc[split_idx:].copy() # ongoing_df now only has A-L columns

    # --- Get Week Start Date (existing logic) ---
    week_start_date = pd.NaT
    sheet_title = worksheet.title
    week_date_match = re.search(r'\((\w+\s\d+)-\d+\)', sheet_title)
    year_match = re.search(r"(?:FSI'|20)(\d{2})", sheet_title, re.IGNORECASE)
    if not year_match and spreadsheet_obj:
        year_match = re.search(r"(?:FSI'|20)(\d{2})", spreadsheet_obj.title, re.IGNORECASE)
    year_found = pd.Timestamp.now().year
    if year_match and len(year_match.groups()) >= 1:
        year_suffix = year_match.group(1)
        year_found = int(f"20{year_suffix}")
    
    # --- Date Assignment Logic (existing logic) ---
    events_df_raw['Assigned_Date'] = pd.NaT
    last_valid_assigned_date = pd.NaT
    for i in events_df_raw.index:
        date_cell_value = events_df_raw.loc[i, date_col_name]
        parsed_numeric_val = pd.to_numeric(date_cell_value, errors='coerce')
        assigned_current_date = pd.NaT
        if pd.notna(parsed_numeric_val):
            if parsed_numeric_val > 1000:
                parsed_dates = Parse_Dates(str(date_cell_value), parsed_numeric_val)
                if parsed_dates and pd.notna(parsed_dates[0]):
                    assigned_current_date = parsed_dates[0]
            elif parsed_numeric_val >= 1 and pd.notna(week_start_date):
                assigned_current_date = week_start_date + pd.Timedelta(days=int(parsed_numeric_val) - 1)
        if pd.notna(assigned_current_date):
            events_df_raw.loc[i, 'Assigned_Date'] = assigned_current_date
            last_valid_assigned_date = assigned_current_date
        else:
            events_df_raw.loc[i, 'Assigned_Date'] = last_valid_assigned_date
    events_df_raw['Assigned_Date'] = events_df_raw['Assigned_Date'].ffill()

    events_df = events_df_raw.dropna(subset=[title_col_name, start_time_col_name], how='all').copy()
    events_df = events_df.dropna(subset=['Assigned_Date']).copy()
    events_df.reset_index(drop=True, inplace=True)

    # --- Sort the processed events (existing logic) ---
    # ADDED: Store the original Start Time string before converting to a numeric for sorting.
    events_df['Original_Start_Time'] = events_df[start_time_col_name].copy()
    
    events_df['numeric_start_time'] = pd.to_numeric(events_df[start_time_col_name], errors='coerce')
    events_df['sort_key'] = events_df['Assigned_Date'] + pd.to_timedelta(events_df['numeric_start_time'], unit='D', errors='coerce')
    events_df.sort_values(by=['sort_key'], inplace=True, na_position='last')

    # --- Format Date and Time (existing logic) ---
    events_df[date_col_name] = events_df['Assigned_Date'].dt.strftime('%A, %B %d')
    
    # MODIFIED: Re-assign the original time strings instead of re-formatting from the numeric value.
    # This prevents the original time strings from being lost.
    # The old line: events_df[start_time_col_name] = events_df['numeric_start_time'].apply(Format_Time)
    events_df[start_time_col_name] = events_df['Original_Start_Time']
    
    # MODIFIED: Drop the new temporary column along with the others.
    events_df.drop(columns=['Assigned_Date', 'numeric_start_time', 'sort_key', 'Original_Start_Time'], inplace=True)

    # --- Recombine and Write Back ---

    # Ensure processed_events_df has all the A-L columns after processing
    # The headers for these are `processed_df_headers`
    processed_events_df = events_df.reindex(columns=processed_df_headers, fill_value='')

    # Create blank_row with A-L columns
    blank_row_df = pd.DataFrame([[''] * len(processed_df_headers)], columns=processed_df_headers)

    # ongoing_df also only has A-L columns
    ongoing_df_processed = ongoing_df.reindex(columns=processed_df_headers, fill_value='')

    # Concatenate the A-L sections
    combined_processed_data_A_to_L = pd.concat([processed_events_df, blank_row_df, ongoing_df_processed], ignore_index=True)

    # Now, prepare the final data to write, combining A-L with M and N.
    # The number of rows in the output might have changed.
    final_output_row_count = len(combined_processed_data_A_to_L)

    # Pad the stored M and N columns to match the new final row count
    padded_stored_col_M = stored_col_M_values + [''] * max(0, final_output_row_count - len(stored_col_M_values))
    padded_stored_col_N = stored_col_N_values + [''] * max(0, final_output_row_count - len(stored_col_N_values))

    data_to_write = []
    for i in range(final_output_row_count):
        row_A_to_L = combined_processed_data_A_to_L.iloc[i].fillna('').tolist()
        
        # Start constructing the full row, preserving original column order
        full_row_data = row_A_to_L # This has columns A-L
        
        # Append or insert M and N at their correct positions (index 12 and 13)
        # This requires careful handling if there are columns beyond N.
        # Assuming M and N are always at index 12 and 13.
        
        # Create a list for the final row with the correct overall width
        # We need the max width of the headers or the data itself
        final_row_width = len(full_deduplicated_headers) # Use the full original headers length

        # Initialize a new row with empty strings
        current_full_output_row = [''] * final_row_width

        # Populate A-L (first 12 elements)
        for col_idx in range(len(row_A_to_L)):
            if col_idx < final_row_width: # Prevent index out of bounds if A-L somehow exceeds final width
                current_full_output_row[col_idx] = row_A_to_L[col_idx]

        # Populate M (index 12)
        if 12 < final_row_width:
            current_full_output_row[12] = padded_stored_col_M[i]
        
        # Populate N (index 13)
        if 13 < final_row_width:
            current_full_output_row[13] = padded_stored_col_N[i]

        # If there are columns beyond N (e.g., O, P, etc.), and you want to preserve them,
        # you'd need to store them as well and re-insert them here.
        # For this logic, we are assuming columns M and N are the only ones to be 'preserved'
        # in their original cell values, and other columns (if any beyond N) would be empty.
        # If you need to preserve original values for columns > N, you'd extend
        # `stored_col_M_values`, `stored_col_N_values` logic.

        data_to_write.append(current_full_output_row)

    # Clear relevant range in worksheet. Use the full original header count for width.
    clear_range = f'A4:{gspread.utils.rowcol_to_a1(worksheet.row_count, len(full_deduplicated_headers))}'
    worksheet.batch_clear([clear_range])

    if data_to_write:
        worksheet.update(range_name='A4', values=data_to_write, value_input_option='USER_ENTERED')
    
    print(f"Successfully processed sheet: '{worksheet.title}'")

def Verbose_Sheet(program, wks_SOG, week_number):
    """
    Main function to connect to Google Sheets and loop through all relevant
    worksheets, organizing each one.
    """
    specific_week = True
    sog_tab = 2 + week_number

    all_worksheets = wks_SOG.worksheets()
    sheets_to_process = []

    if specific_week:
        if 0 <= sog_tab < len(all_worksheets):
            # Get the specific worksheet by its index
            worksheet_to_add = all_worksheets[sog_tab]
            if worksheet_to_add.title not in ["Welcome!", "Template"]:
                sheets_to_process.append(worksheet_to_add.title)
                print(f"Processing only specified week: '{worksheet_to_add.title}' (Tab Index: {sog_tab})")
            else:
                print(f"Skipping specified worksheet '{worksheet_to_add.title}' (Tab Index: {sog_tab}) as it's a excluded sheet.")
        else:
            print(f"Error: Specified target tab index {sog_tab} is out of bounds for the number of worksheets available ({len(all_worksheets)}).")
            return # Exit if the specific week is invalid
    else:
        # Process all sheets except Welcome! and Template
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
    """
    Main function to connect to Google Sheets and loop through all relevant
    worksheets, organizing each one.
    """
    specific_week = True
    sog_tab = 2 + week_number

    all_worksheets = wks_SOG.worksheets()
    sheets_to_process = []

    if specific_week:
        if 0 <= sog_tab < len(all_worksheets):
            # Get the specific worksheet by its index
            worksheet_to_add = all_worksheets[sog_tab]
            if worksheet_to_add.title not in ["Welcome!", "Template"]:
                sheets_to_process.append(worksheet_to_add.title)
                print(f"Processing only specified week: '{worksheet_to_add.title}' (Tab Index: {sog_tab})")
            else:
                print(f"Skipping specified worksheet '{worksheet_to_add.title}' (Tab Index: {sog_tab}) as it's a excluded sheet.")
        else:
            print(f"Error: Specified target tab index {sog_tab} is out of bounds for the number of worksheets available ({len(all_worksheets)}).")
            return # Exit if the specific week is invalid
    else:
        # Process all sheets except Welcome! and Template
        sheets_to_process = [s.title for s in all_worksheets if s.title not in ["Welcome!", "Template"]]
        print("Processing all sheets except 'Welcome!' and 'Template'.")

    for sheet_name in sheets_to_process:
        worksheet = wks_SOG.worksheet(sheet_name)
        try:
            Organize_Sheet(worksheet, wks_SOG) 
        except Exception as e:
            print(f"!!! An error occurred while processing sheet '{sheet_name}': {e}")
            
    print('\nAll sheets processed.')