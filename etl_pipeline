import paramiko
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Load variables
load_dotenv()

# --- CONFIGURATION ---
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 2221))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
REMOTE_DIR = os.getenv("REMOTE_PATH", "/") 

# PATH SETUP
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DIR = os.path.join(SCRIPT_DIR, "downloads")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "csv")  # <--- CHANGED: Now points to 'csv' folder
HISTORY_FILE = os.path.join(SCRIPT_DIR, "processed_history.log")

# SAFETY LIMIT
LOOKBACK_DAYS = 30 

def create_sftp_client(host, port, user, password):
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp, transport
    except Exception as e:
        print(f"Error connecting to SFTP: {e}")
        return None, None

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return set()
    with open(HISTORY_FILE, 'r') as f:
        return set(line.strip() for line in f)

def update_history(new_files):
    with open(HISTORY_FILE, 'a') as f:
        for file in new_files:
            f.write(f"{os.path.basename(file)}\n")

def download_new_files(sftp, history_set):
    if not os.path.exists(STAGING_DIR):
        os.makedirs(STAGING_DIR)

    print(f"Listing files in {REMOTE_DIR}...")
    try:
        all_files = sftp.listdir(REMOTE_DIR)
    except Exception as e:
        print(f"Error listing remote files: {e}")
        return []

    target_files = []
    today = datetime.now()
    cutoff_date = today - timedelta(days=LOOKBACK_DAYS)
    
    print("Checking for new files...")
    for f in all_files:
        if not f.endswith('.xml'):
            continue
        
        if f in history_set:
            continue

        try:
            parts = f.split('_')
            if len(parts) > 2:
                date_str = parts[2] 
                if len(date_str) == 6 and date_str.isdigit():
                    file_date = datetime.strptime(date_str, "%y%m%d")
                    if file_date >= cutoff_date:
                        target_files.append(f)
        except Exception:
            continue

    print(f"Found {len(target_files)} NEW files that haven't been processed yet.")

    local_paths = []
    for file in target_files:
        remote_path = os.path.join(REMOTE_DIR, file)
        local_path = os.path.join(STAGING_DIR, file)
        
        if not os.path.exists(local_path):
            print(f"Downloading {file}...")
            sftp.get(remote_path, local_path)
        
        local_paths.append(local_path)
    
    return local_paths

def calculate_hours(start_str, end_str):
    if not start_str or not end_str:
        return None
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        t1 = datetime.strptime(start_str, fmt)
        t2 = datetime.strptime(end_str, fmt)
        diff = t2 - t1
        return round(diff.total_seconds() / 3600, 2)
    except Exception:
        return None

def parse_citilink_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        data = {}
        data['filename'] = os.path.basename(file_path)

        def find_node(parent, tag_name):
            if parent is None: return None
            for child in parent.iter(): 
                if child.tag.endswith(tag_name):
                    return child
            return None

        # --- 1. HEADER & FLIGHT ID ---
        flight_node = find_node(root, "Flight")
        if flight_node is not None:
            data['flight_origin_date'] = flight_node.get('flightOriginDate')
            
            flight_id_node = find_node(flight_node, "FlightIdentifier")
            data['flight_id'] = flight_id_node.text if flight_id_node is not None else ""
            
            comm_num_node = find_node(flight_node, "CommercialFlightNumber")
            data['flight_number'] = comm_num_node.text if comm_num_node is not None else ""
            
            dep_node = find_node(flight_node, "DepartureAirport")
            if dep_node is not None:
                data['origin_iata'] = find_node(dep_node, "AirportIATACode").text

            arr_node = find_node(flight_node, "ArrivalAirport")
            if arr_node is not None:
                data['dest_iata'] = find_node(arr_node, "AirportIATACode").text

        # --- 2. AIRCRAFT ---
        aircraft_node = find_node(root, "Aircraft")
        if aircraft_node is not None:
            data['aircraft_reg'] = aircraft_node.get('aircraftRegistration')

        # --- 3. TIMES & CALCULATIONS ---
        report_node = find_node(root, "GeneralFlightReport")
        if report_node is not None:
            out_node = find_node(report_node, "Out")
            data['out_time'] = find_node(out_node, "Time").get('time') if out_node else None
            data['fuel_out'] = find_node(out_node, "FuelOnBoard").get('fuelMass') if out_node else None
            
            off_node = find_node(report_node, "Off")
            data['off_time'] = find_node(off_node, "Time").get('time') if off_node else None

            on_node = find_node(report_node, "On")
            data['on_time'] = find_node(on_node, "Time").get('time') if on_node else None

            in_node = find_node(report_node, "In")
            data['in_time'] = find_node(in_node, "Time").get('time') if in_node else None
            data['fuel_in'] = find_node(in_node, "FuelOnBoard").get('fuelMass') if in_node else None

            data['block_hour'] = calculate_hours(data['out_time'], data['in_time'])
            data['air_time'] = calculate_hours(data['off_time'], data['on_time'])

        # --- 4. OPERATION DETAILS ---
        op_node = find_node(root, "Operation")
        if op_node is not None:
            fuel_report = find_node(op_node, "FuelReport")
            if fuel_report:
                refuel_node = find_node(fuel_report, "RefuelingAction")
                if refuel_node:
                    uplift_node = find_node(refuel_node, "FuelUplift")
                    if uplift_node is not None:
                        data['fuel_uplift'] = uplift_node.get('fuelUplift')

            ft_node = find_node(op_node, "FlightType")
            if ft_node is not None:
                data['flight_type'] = ft_node.get('flightType')

            to_node = find_node(op_node, "WhoDidTheTakeOff")
            if to_node is not None: data['pilot_takeoff'] = to_node.get('pilotName')

            land_node = find_node(op_node, "WhoDidTheLanding")
            if land_node is not None: data['pilot_landing'] = land_node.get('pilotName')

            act_arr_node = find_node(op_node, "ActualArrivalAirport")
            if act_arr_node is not None:
                data['arrival_type'] = act_arr_node.get('ArrivalAirportType')

        # --- 5. CREW LIST ---
        crew_list_node = find_node(root, "CrewListDetails")
        formatted_crew = []

        if crew_list_node is not None:
            for crew_info in crew_list_node.iter():
                if crew_info.tag.endswith("CrewInfo"):
                    name = ""
                    for pi in crew_info.iter():
                        if pi.tag.endswith("PersonalInfo") and pi.get("surname"):
                            name = pi.get("surname")
                            break
                    
                    rank = ""
                    emp_id = ""
                    crew_data = find_node(crew_info, "Crew")
                    if crew_data is not None:
                        rank = crew_data.get('rank', '')
                        emp_id = (crew_data.get('employeeId') or crew_data.get('staffNumber') or "")

                    if name:
                        entry = f"{rank} {name} ({emp_id})".strip()
                        formatted_crew.append(entry)

        data['crew_details'] = ", ".join(formatted_crew)
        return data

    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return None

def process_and_export(file_list):
    # Ensure CSV directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_data = []
    print(f"\n--- PROCESSING {len(file_list)} FILES ---")

    for file_path in file_list:
        row = parse_citilink_xml(file_path)
        if row:
            all_data.append(row)

    if all_data:
        df = pd.DataFrame(all_data)
        
        key_cols = [
            'flight_origin_date', 'flight_number', 'flight_type', 'arrival_type',
            'origin_iata', 'dest_iata', 'aircraft_reg',
            'block_hour', 'air_time',
            'out_time', 'in_time', 'off_time', 'on_time', 
            'fuel_out', 'fuel_in', 'fuel_uplift',
            'pilot_takeoff', 'pilot_landing',
            'crew_details', 
            'filename', 'flight_id' 
        ]
        
        existing_keys = [c for c in key_cols if c in df.columns]
        other_keys = [c for c in df.columns if c not in existing_keys]
        df = df[existing_keys + other_keys]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"New_Flights_Found_{timestamp}.csv"
        output_path = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(output_path, index=False)
        print(f"\n✅ SUCCESS! {len(df)} new flights saved to: {output_path}")
        return True
    else:
        print("\nNo valid data found in downloaded files.")
        return False

def run_pipeline():
    print("--- SMART DOWNLOADER STARTED ---")
    
    history = load_history()
    print(f"History loaded: {len(history)} files previously processed.")

    sftp, transport = create_sftp_client(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS)
    
    local_files = []
    if sftp:
        local_files = download_new_files(sftp, history)
        sftp.close()
        transport.close()
    else:
        print("Could not connect to SFTP.")
        return

    if not local_files:
        print("No new files found. Everything is up to date!")
        return

    success = process_and_export(local_files)

    if success:
        print("\n--- CLEANUP PHASE ---")
        update_history(local_files)
        print("History log updated.")

        for f in local_files:
            try:
                os.remove(f)
            except Exception:
                pass
        print("Local XML files deleted.")

if __name__ == "__main__":
    run_pipeline()
