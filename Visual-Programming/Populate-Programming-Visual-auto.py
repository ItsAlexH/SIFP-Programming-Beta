import numpy as np
import pandas as pd
import datetime
import gspread
from dotenv import load_dotenv
import os
import time
import sys
import warnings

# --- Import custom functions from OrgParse and OrgProg ---
try:
    from OrgParse import conversion_excel_date as excel_date_converter
    from OrgParse import parse_times, get_color, clear_dates, post_events
    from OrgProg import prog_weeks, get_programming, sog_days, clean_headers
except ImportError as e:
    print(f"Error importing custom modules: {e}")
    sys.exit(1)

# --- Configuration & Setup ---
load_dotenv()

# --- Custom Functions (Corrected and Refined) ---
def conversion_excel_date(f):
    """
    Converts an Excel serial date number to a datetime.date object.
    Handles both integer (date only) and float (date with time) values.
    """
    if not isinstance(f, (int, float)):
        try:
            f = float(f)
        except (ValueError, TypeError):
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

    return (temp + full_days_td + seconds_td).date()

def parse_time_to_minutes(time_str):
    """
    Parses a time string and returns the number of minutes since midnight.
    Handles 'HH:MM AM/PM' and 'HH:MM' formats.
    """
    if not isinstance(time_str, str) or not time_str.strip():
        return None
    time_str = time_str.strip().upper().replace('.', '')
    try:
        # Handle HH:MM PM/AM format
        dt_obj = datetime.datetime.strptime(time_str, '%I:%M %p')
    except ValueError:
        try:
            # Handle HH:MM military time format
            dt_obj = datetime.datetime.strptime(time_str, '%H:%M')
        except ValueError:
            return None
    
    return dt_obj.hour * 60 + dt_obj.minute

def excel_time_to_minutes(excel_time):
    """
    Converts an Excel time serial number to minutes since midnight.
    """
    if not isinstance(excel_time, (int, float)):
        try:
            excel_time = float(excel_time)
        except (ValueError, TypeError):
            return None
    
    return int(round(excel_time * 24 * 60))

# --- Main Script Logic Encapsulated in a Function ---
def run_script_logic(week_num):
    # Google Sheets setup
    try:
        gc = gspread.service_account(filename='service_account.json')
        
        # --- Retry loop for Google Sheet connection ---
        retries = 3
        delay = 5  # seconds
        for i in range(retries):
            try:
                wks_submit = gc.open(os.getenv("SUBMITTED_EVENTS_TOKEN"))
                break
            except gspread.exceptions.APIError as e:
                if "Internal Error" in str(e) and i < retries - 1:
                    print(f"Temporary Google Sheets API error [500]: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise
        else:
            print("Failed to connect to Google Sheets after multiple retries. Exiting.")
            return
            
    except gspread.exceptions.APIError as e:
        print(f"Error connecting to Google Sheets: {e}")
        print("Please ensure 'service_account.json' is valid and has access to the spreadsheet.")
        return

    # --- Process Submission Data (Worksheet 0) ---
    print("--- Processing Submission Data ---")
    try:
        submit_raw_values = wks_submit.get_worksheet(0).get_all_values(value_render_option='UNFORMATTED_VALUE')
        submit_raw_headers = submit_raw_values[0]
        submit_data_rows = submit_raw_values[1:]

        submit_cleaned_headers = clean_headers(submit_raw_headers, "Submit_Unnamed")
        prog_data = pd.DataFrame(submit_data_rows, columns=submit_cleaned_headers)

        Titles = prog_data.get('Event Title', pd.Series(dtype=str)).tolist()
        Dates = prog_data.get('Event Date', pd.Series(dtype=str)).tolist()
        Start_Times = prog_data.get('Start Time', pd.Series(dtype=str)).tolist()
        End_Times = prog_data.get('End Time', pd.Series(dtype=str)).tolist()
        Locations = prog_data.get('Location', pd.Series(dtype=str)).tolist()

        # Parse Dates
        for i in range(len(Dates)):
            if isinstance(Dates[i], datetime.date):
                continue
            if isinstance(Dates[i], str) and Dates[i].strip():
                try:
                    Dates[i] = datetime.datetime.strptime(Dates[i], '%Y-%m-%d').date()
                    continue
                except ValueError:
                    pass
                try:
                    Dates[i] = datetime.datetime.strptime(Dates[i], '%m/%d/%Y').date()
                    continue
                except ValueError:
                    pass
            elif isinstance(Dates[i], (int, float)):
                Dates[i] = conversion_excel_date(Dates[i])
            else:
                Dates[i] = None

        # Parse Times
        parsed_start_times = [parse_time_to_minutes(t) for t in Start_Times]
        parsed_end_times = [parse_time_to_minutes(t) for t in End_Times]

    except gspread.exceptions.APIError as e:
        print(f"Error reading submission worksheet (Worksheet 0): {e}")
        return
    except KeyError as e:
        print(f"Column not found in submission data: {e}")
        print("Please check the column headers in your Google Sheet (Worksheet 0) and update the script if necessary.")
        print(f"Available headers: {submit_cleaned_headers}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during submission data processing: {e}")
        return

    print(f"Processed {len(Titles)} events from submission data.")

    # --- Process Programming Visual Data (Worksheet week + 1) ---
    print(f"\n--- Processing Programming Visual Data for Week {week_num} (Worksheet {week_num+1}) ---")

    try:
        wks_prog = wks_submit.get_worksheet(week_num + 1-3)
        prog_visual_raw_values = wks_prog.get_all_values(value_render_option='UNFORMATTED_VALUE')
        prog_visual_raw_headers = prog_visual_raw_values[0]
        prog_visual_data_rows = prog_visual_raw_values[1:]

        prog_visual_cleaned_headers = clean_headers(prog_visual_raw_headers, "Visual_Unnamed")
        prog_visual = pd.DataFrame(prog_visual_data_rows, columns=prog_visual_cleaned_headers)

        headers_prog = prog_visual_cleaned_headers.copy()
        headers_prog_str = prog_visual_cleaned_headers.copy()

        for i in range(len(headers_prog)):
            header_val = headers_prog[i]
            if isinstance(header_val, str):
                try:
                    numeric_val = int(header_val)
                    headers_prog[i] = conversion_excel_date(numeric_val)
                    continue
                except (ValueError, TypeError):
                    pass

            if isinstance(headers_prog[i], str):
                try:
                    parsed_date = pd.to_datetime(headers_prog[i], errors='coerce')
                    if pd.notna(parsed_date):
                        headers_prog[i] = parsed_date.date()
                except Exception:
                    pass

        print(f"Visual sheet headers (cleaned_str): {headers_prog_str}")
        print(f"Visual sheet headers (parsed_dates): {headers_prog}")

        first_date_col_name = None
        for h_str, h_obj in zip(headers_prog_str, headers_prog):
            if isinstance(h_obj, datetime.date):
                first_date_col_name = h_str
                break

        if first_date_col_name:
            print(f"\nContent of column '{first_date_col_name}':")
            if first_date_col_name in prog_visual.columns:
                print(prog_visual[first_date_col_name].head())
            else:
                print(f"Error: Column '{first_date_col_name}' not found in prog_visual DataFrame despite being in headers_prog_str.")
        else:
            print("\nCould not find a date column to display example content.")
            if prog_visual_cleaned_headers and prog_visual_cleaned_headers[0] in prog_visual.columns:
                print(f"\nContent of first cleaned column ('{prog_visual_cleaned_headers[0]}'):")
                print(prog_visual[prog_visual_cleaned_headers[0]].head())

        date_headers_only = [h for h in headers_prog if isinstance(h, datetime.date)]
        if date_headers_only:
            week_min_date = min(date_headers_only)
            week_max_date = max(date_headers_only)
            print(f"\nWeek Min Date: {week_min_date}, Week Max Date: {week_max_date}")
        else:
            week_min_date = None
            week_max_date = None
            print("\nNo valid date headers found in the visual programming sheet.")

        Mask = np.zeros(len(Dates), dtype=int)
        date_to_col_idx = {d: i for i, d in enumerate(headers_prog) if isinstance(d, datetime.date)}

        for i, event_date in enumerate(Dates):
            if isinstance(event_date, datetime.date) and event_date in date_to_col_idx:
                Mask[i] = date_to_col_idx[event_date]

        print(f"\nMask (Column Indices for Event Dates): {Mask}")

    except gspread.exceptions.APIError as e:
        print(f"Error reading programming visual worksheet (Worksheet {week_num+1}): {e}")
        return
    except KeyError as e:
        print(f"Column not found in visual data: {e}")
        print(f"Available headers in visual sheet: {prog_visual_cleaned_headers}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during programming visual data processing: {e}")
        return

    # Parse Calendar_Times
    Calendar_Times = prog_visual[prog_visual_cleaned_headers[0]].tolist()
    # Convert calendar times to a consistent numerical format
    parsed_calendar_times = [excel_time_to_minutes(t) if isinstance(t, (int, float)) else parse_time_to_minutes(t) for t in Calendar_Times]

    print("\n--- Updating Google Sheet with Events ---")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="gspread")

    for i, event_date in enumerate(Dates):
        event_title = Titles[i].strip()
        start_time_minutes = parsed_start_times[i]
        end_time_minutes = parsed_end_times[i]

        col_idx_0_based = Mask[i]

        if col_idx_0_based != 0 and col_idx_0_based < len(headers_prog_str):
            event_col_name = headers_prog_str[col_idx_0_based]

            if event_col_name not in prog_visual.columns:
                print(f"Skipping event '{event_title}' (Date: {event_date}): Column '{event_col_name}' not found in visual DataFrame.")
                continue

            ii_s = -1
            ii_e = -1
            if start_time_minutes is not None and end_time_minutes is not None:
                # Find the index of the start and end times
                if start_time_minutes in parsed_calendar_times:
                    ii_s = parsed_calendar_times.index(start_time_minutes)
                if end_time_minutes in parsed_calendar_times:
                    ii_e = parsed_calendar_times.index(end_time_minutes)

            if ii_s != -1 and ii_e != -1 and ii_s <= ii_e:
                gspread_start_row = ii_s + 2
                gspread_end_row = ii_e + 2
                gspread_col = col_idx_0_based + 1

                start_cell_a1 = gspread.utils.rowcol_to_a1(gspread_start_row, gspread_col)
                end_cell_a1 = gspread.utils.rowcol_to_a1(gspread_end_row, gspread_col)

                range_to_update = f"{start_cell_a1}:{end_cell_a1}"

                print(f"Processing range '{range_to_update}' for event '{event_title}'...")

                try:
                    current_range_values = wks_prog.get(range_to_update)

                    expected_rows_in_range = (gspread_end_row - gspread_start_row + 1)
                    if not isinstance(current_range_values, list) or not all(isinstance(row, list) for row in current_range_values):
                        current_range_values = [[''] for _ in range(expected_rows_in_range)]
                    elif len(current_range_values) < expected_rows_in_range:
                        current_range_values.extend([['']] * (expected_rows_in_range - len(current_range_values)))

                    new_values_to_write = []

                    for k in range(expected_rows_in_range):
                        current_cell_content = current_range_values[k][0] if current_range_values[k] else ''
                        current_cell_content_str = str(current_cell_content).strip()

                        existing_titles_in_cell = [t.strip() for t in current_cell_content_str.split(';') if t.strip()]

                        if event_title in existing_titles_in_cell:
                            new_values_to_write.append([current_cell_content_str])
                            print(f" - Cell R{gspread_start_row + k}C{gspread_col}: '{event_title}' already present. No change.")
                        elif not current_cell_content_str:
                            new_values_to_write.append([event_title])
                            print(f" - Cell R{gspread_start_row + k}C{gspread_col}: Empty, adding '{event_title}'.")
                        else:
                            new_values_to_write.append([f"{current_cell_content_str}; {event_title}"])
                            print(f" - Cell R{gspread_start_row + k}C{gspread_col}: Appending '{event_title}' to '{current_cell_content_str}'.")

                    wks_prog.update(range_name=range_to_update, values=new_values_to_write)
                    print(f"Successfully updated cells for event '{event_title}'.")

                except Exception as e:
                    print(f"Failed to update cells for event '{event_title}' in range '{range_to_update}': {e}")
            else:
                print(f"Skipping update for event '{event_title}' (Date: {event_date}): Could not find valid start/end times in Calendar_Times or invalid range ({ii_s}-{ii_e}).")
        else:
            print(f"Skipping event '{event_title}' (Date: {event_date}): Event date not found in visual sheet headers or Mask[i] is 0/invalid index ({col_idx_0_based}).")

    print("\nScript execution complete.")


# --- Main Loop to Run Script Hourly ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            initial_week = int(sys.argv[1])
        except ValueError:
            print("Invalid argument for 'n'. Please provide an integer.")
            sys.exit(1)
    else:
        initial_week = 1
        print(f"No week number provided as argument. Using default week: {initial_week}")

    while True:
        print(f"\n--- Starting new run at {datetime.datetime.now()} for Week {initial_week} ---")
        run_script_logic(initial_week)
        print(f"--- Finished run at {datetime.datetime.now()}. Sleeping for 60 minutes... ---")
        time.sleep(360)