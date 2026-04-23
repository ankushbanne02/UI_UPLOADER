import re
import json
import os


from datetime import datetime,timedelta

ID_MAP = {
    "1": "ItemRegister",
    "2": "ItemPropertiesUpdate",
    "3": "ItemInstruction",
    "5": "UnverifiedSortReport",
    "6": "VerifiedSortReport",
    "7": "ItemDeRegister",
    "98": "WatchdogReply",
    "99": "WatchdogRequest",
}


chute_map= {
    "1": "CHUTE01",
    "2": "CHUTE02",
    "3": "CHUTE03",
    "4": "CHUTE04",
    "5": "CHUTE05",
    "6": "CHUTE06",
    "7": "CHUTE07",
    "8": "CHUTE08",
    "9": "CHUTE09",
    "10": "CHUTE10",
    "11": "CHUTE11",
    "12": "CHUTE12",
    "13": "CHUTE13",
    "14": "CHUTE14",
    "15": "CHUTE15",
    "16": "CHUTE16",
    "17": "CHUTE17",
    "18": "CHUTE18",
    "19": "CHUTE19",
    "20": "CHUTE20",
    "21": "CHUTE21",
    "22": "CHUTE22",
    "23": "CHUTE23",
    "24": "CHUTE24",
    "25": "CHUTE25",
    "26": "CHUTE26",
    "27": "CHUTE27",
    "28": "CHUTE28",
    "29": "CHUTE29",
    "30": "CHUTE30",
    "31": "CHUTE31",
    "32": "CHUTE32",
    "33": "CHUTE33",
    "34": "CHUTE34",
    "35": "CHUTE35",
    "51": "CHUTE51",
    "52": "CHUTE52",
    "53": "CHUTE53",
    "54": "CHUTE54",
    "55": "CHUTE55",
    "56": "CHUTE56",
    "57": "CHUTE57",
    "58": "CHUTE58",
    "59": "CHUTE59",
    "60": "CHUTE60",
    "61": "CHUTE61",
    "62": "CHUTE62",
    "63": "CHUTE63",
    "64": "CHUTE64",
    "65": "CHUTE65",
    "66": "CHUTE66",
    "67": "CHUTE67",
    "68": "CHUTE68",
    "69": "CHUTE69",
    "70": "CHUTE70",
    "71": "CHUTE71",
    "72": "CHUTE72",
    "73": "CHUTE73",
    "74": "CHUTE74",
    "75": "CHUTE75",
    "76": "CHUTE76",
    "77": "CHUTE77",
    "78": "CHUTE78",
    "79": "CHUTE79",
    "80": "CHUTE80",
    "81": "CHUTE81",
    "82": "CHUTE82",
    "83": "CHUTE83",
    "84": "CHUTE84",
    "85": "CHUTE85",
    "36": "CHUTE36"
}




# --- Regex helpers -------------------------------------------------
LOG_DATE = re.compile(r'^(\d{4}-\d{2}-\d{2})')

# Match the time and milliseconds: "09:15:55,970"
LOG_TIME = re.compile(r'\s((\d{2}:\d{2}:\d{2}),\d{3})')

# Match the raw body after "): " and before " []" at the end
RAW_BODY = re.compile(r'\): (.*?)(?: \[\]$)')



# --- Main parser ---------------------------------------------------
def parse_log_lines(text: str):
    active_hostid_parcels = {}   
    active_pic_only_parcels = {} 
    all_parcel_records = []
    anomalies_records=[]

    for line in text.splitlines():
        date_m, body_m, ts_m= LOG_DATE.search(line), RAW_BODY.search(line),LOG_TIME.search(line)
        if not (ts_m and body_m and date_m):
            continue
        
        parts = body_m.group(1).strip().split("|")
        if len(parts) < 6 or ID_MAP.get(parts[3], "").startswith("Watchdog"):
            continue

        try:
            current_pic = int(parts[4])
        except ValueError:
            continue

        current_host_id = parts[5].strip()
        msg_id=parts[3].strip()
        msg = ID_MAP.get(parts[3], f"Type{parts[3]}")

        joined_line = "|".join(parts)
        z_time_match = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', joined_line)
        if z_time_match:
            ts_iso = z_time_match.group(0).replace("Z", "")
        else:
            ms = ts_m.group(2).zfill(3)
            ts_iso = f"{ts_m.group(1)}.{ms}".replace(" ", "T")

        date_part, time_part = ts_iso.split("T")

        location=None

        if msg_id:
            if msg_id=="1":
                location=parts[6].strip()
            elif msg_id=="2":

                location=parts[6].strip()

            elif msg_id=="5":
                location=parts[6].strip()

            elif msg_id=="6":
                location=parts[6].strip()

            elif msg_id=="7":
                location=parts[6].strip()

        if msg_id=="3":
            if parts[6]=="" and parts[7]=="":
                msg=msg+"(HOST_REPLY)"
            else:
                msg=msg+"(DESTINATION_REPLY)"

        raw=date_m.group(1)+" "+ts_m.group(1)+"|"+"|".join(parts)
        

        event = {
            "date": date_m.group(1),
            "ts": ts_m.group(1),
            "msg_id":msg_id,
            "type": msg,
            "location":location,
            "raw": raw
        }

        target_parcel = None

        if current_host_id:
            if current_host_id in active_hostid_parcels:
                target_parcel = active_hostid_parcels[current_host_id]
                if current_pic in active_pic_only_parcels and active_pic_only_parcels[current_pic] is target_parcel:
                    del active_pic_only_parcels[current_pic]

            elif current_pic in active_pic_only_parcels:
                target_parcel = active_pic_only_parcels[current_pic]
                target_parcel["hostId"] = current_host_id
                active_hostid_parcels[current_host_id] = target_parcel

                if target_parcel.get("anomalies_exists"):
                    for anomaly in target_parcel["anomalies"]:
                        if anomaly.get("host_id") in (None, "None", ""):
                            anomaly["host_id"] = current_host_id

                del active_pic_only_parcels[current_pic]

            else:
                new_parcel = {
                    "hostId": current_host_id,
                    "pic": current_pic,
                    "date": date_part,
                    "registerTS": None,
                    "latestTS":None,
                    "closedTS": None,
                    "status": "open",
                    "identificationTS":None,
                    "identification_location":None,
                    "sortationTS":None,
                    "exitTS":None,
                    "exit_location":None,
                    "plc_number": None,
                    "Registered_location": None,
                    "customer_location": None,
                    "sort_strategy": None,
                    "barcode_data": {
                        "barcodes": [],
                        "barcode_count": 0,
                        "barcode_state": None
                    },
                    "barcode_error": False,
                    "alibi_id": None,
                    "volume_data": {
                        "volume_state": None,
                        "length": None,
                        "width": None,
                        "height": None,
                        "box_volume": None,
                        "real_volume": None
                    },
                    "volume_error": False,
                    "item_state": None,    
                    "actual_destination": None,
                    "destinations": [],
                    "destination_status": {},
                    "sort_code": None,
                    "entrance_state": None,
                    "exit_state": None,
                    "deregister":None,
                    "anomalies_exists": False,
                    "anomalies": [],
                    "complete_record":False,
                    "events": []
                }
                all_parcel_records.append(new_parcel)
                active_hostid_parcels[current_host_id] = new_parcel
                target_parcel = new_parcel

        else:
            if msg == "ItemRegister":
                new_parcel = {
                    "hostId": None,
                    "pic": current_pic,
                    "date": None,
                    "registerTS": None,
                    "latestTS":None,
                    "closedTS": None,
                    "status": "open",
                    "identificationTS":None,
                    "identification_location":None,
                    "sortationTS":None,
                    "exitTS":None,
                    "exit_location":None,
                    "plc_number": None,
                    "Registered_location": None,
                    "customer_location": None,
                    "sort_strategy": None,
                    "barcode_data": {
                        "barcodes": [],
                        "barcode_count": 0,
                        "barcode_state": None
                    },
                    "barcode_error": False,
                    "alibi_id": None,
                    "volume_data": {
                        "volume_state": None,
                        "length": None,
                        "width": None,
                        "height": None,
                        "box_volume": None,
                        "real_volume": None
                    },
                    "volume_error": False,
                    "item_state": None,
                    "actual_destination": None,
                    "destinations": [],
                    "destination_status": {},
                    "sort_code": None,
                    "entrance_state": parts[8].strip(),
                    "exit_state": None,
                    "deregister":None,
                    "anomalies_exists": False,
                    "anomalies": [],
                    "complete_record":False,
                    "events": []
                }
                all_parcel_records.append(new_parcel)
                active_pic_only_parcels[current_pic] = new_parcel
                target_parcel = new_parcel

                target_parcel["Registered_location"] = parts[6].strip()
                if parts[7].strip():
                    target_parcel["customer_location"] = parts[7].strip()
                target_parcel["registerTS"] = ts_m.group(1)
                target_parcel["latestTS"]=ts_m.group(1)
                target_parcel["plc_number"] = parts[0].strip()
                target_parcel["date"] = date_part
                if target_parcel["entrance_state"]:
                    if target_parcel["entrance_state"] not in ["1", "2"]:
                        target_parcel["anomalies_exists"] = True
                        target_parcel["anomalies"].append({
                            "host_id": "None",
                            "source_message_type": msg,
                            "anomaly_timestamp": date_m.group(1)+" " + ts_m.group(1),
                            "anomaly_description": (
                            f"Entrance value invalid anomaly. "
                            f"Entrance state must be 1 or 2, but found {target_parcel['entrance_state']}."),
                            "raw_log_entry": raw})
            else:
                if current_pic in active_pic_only_parcels:
                    target_parcel = active_pic_only_parcels[current_pic]
                else:
                    continue

        if target_parcel:
            target_parcel["events"].append(event)
            target_parcel["latestTS"]=ts_m.group(1)

            if msg == "ItemPropertiesUpdate":
                if target_parcel["date"]==None:
                    target_parcel["date"]=date_part

        

                barcode_body = parts[9].strip()
                temp_barcodes = []

                if barcode_body:
                    barcode_fields = barcode_body.split(';')
                    if len(barcode_fields) >= 3:
                        barcode_string_field = barcode_fields[2].strip()
                        if barcode_string_field:
                            individual_barcode_strings = barcode_string_field.split('@')
                            for bc_str in individual_barcode_strings:
                                stripped_bc_str = bc_str.strip()
                                if stripped_bc_str.startswith("0]C"):
                                    stripped_bc_str=stripped_bc_str.removeprefix("0]C")
                                    temp_barcodes.append(stripped_bc_str)

                target_parcel["barcode_data"]["barcodes"] = temp_barcodes
                target_parcel["barcode_data"]["barcode_count"] = len(temp_barcodes)
                target_parcel["barcode_data"]["barcode_state"] = int(barcode_fields[0])
                target_parcel["identificationTS"]=ts_m.group(1)
                target_parcel["latestTS"]=ts_m.group(1)
                target_parcel["identification_location"]=location

                if target_parcel["barcode_data"]["barcode_state"] != 6:
                    target_parcel["barcode_error"] = True

                temp_alibi_id = parts[11].strip()
                if temp_alibi_id:
                    target_parcel["alibi_id"] = temp_alibi_id

                temp_volume_data = parts[12].strip()
                if temp_volume_data:
                    volume_fields = temp_volume_data.split(';')
                    if len(volume_fields) >= 7:
                        target_parcel["volume_data"]["volume_state"] = int(volume_fields[0])
                        target_parcel["volume_data"]["length"] = int(volume_fields[2])
                        target_parcel["volume_data"]["width"] = int(volume_fields[3])
                        target_parcel["volume_data"]["height"] = int(volume_fields[4])
                        target_parcel["volume_data"]["box_volume"] = int(volume_fields[5])
                        target_parcel["volume_data"]["real_volume"] = int(volume_fields[6])

                        if target_parcel["volume_data"]["volume_state"] != 6:
                            target_parcel["volume_error"] = True

            elif msg == "ItemInstruction(DESTINATION_REPLY)":
                if target_parcel["date"]==None:
                    target_parcel["date"]=date_part
                if parts[6].strip():
                    target_parcel["sort_strategy"] = parts[6].strip()

                temp_destinations = []

                if parts[7].strip():
                    destinations_list = [d.strip() for d in parts[7].split(';') if d.strip()]
                    temp_destinations.extend(destinations_list)

                target_parcel["latestTS"]=ts_m.group(1)
        
                target_parcel["destinations"] = temp_destinations

           


            elif msg == "VerifiedSortReport":
                if target_parcel["date"]==None:
                    target_parcel["date"]=date_part
                target_parcel["latestTS"]=ts_m.group(1)
                target_parcel["sortationTS"]=ts_m.group(1)
                temp_actual_destination = parts[9].strip() if len(parts) > 9 else None
                if temp_actual_destination:
                    chute_name = chute_map.get(temp_actual_destination.lstrip("0"), temp_actual_destination)

                    target_parcel["actual_destination"] = chute_name

                temp_destination_status = parts[10].strip() if len(parts) > 10 else None
                if temp_destination_status:
                    destination_status_dict = {}
                    values = temp_destination_status.split(";")
                    
                    for i in range(0, len(values) - 1, 2):
                        try:
                            key = values[i].lstrip("0")  # remove leading zeros
                            value = int(values[i + 1])
                            chute_name = chute_map.get(key, key)
                            destination_status_dict[chute_name] = value
                        except ValueError:
                            continue
                    target_parcel["destination_status"] = destination_status_dict

                    raw_actual_dest = temp_actual_destination.lstrip("0") if temp_actual_destination else None
                    if raw_actual_dest and chute_map.get(raw_actual_dest) in destination_status_dict:
                        target_parcel["sort_code"] = destination_status_dict[chute_map[raw_actual_dest]]

                    if temp_actual_destination == "999":
                        if destination_status_dict:
                            last_key = list(destination_status_dict)[-1]
                            target_parcel["sort_code"] = destination_status_dict[last_key]

                    if target_parcel["actual_destination"]=="999":
                        last_key = list(destination_status_dict)[-1]
                        last_value = destination_status_dict[last_key]
                        target_parcel["sort_code"]=last_value

                if len(parts) > 7 and parts[7].strip():
                    destinations_list = [d.strip().lstrip("0") for d in parts[7].split(';') if d.strip()]
                    mapped_destinations = [chute_map.get(d, d) for d in destinations_list]
                    target_parcel["destinations"] = mapped_destinations

                if target_parcel["sort_code"] == 1 and target_parcel["actual_destination"] != "999":
                    target_parcel["status"] = "sorted"


                elif target_parcel.get("identificationTS") is not None and target_parcel["actual_destination"] !=999 and target_parcel["sort_code"]!= 1:
                    target_parcel["status"]="failed_to_sort"

                elif len(target_parcel["destinations"]) == 1  and "999" in target_parcel["destinations"]:
                    target_parcel["status"]="sorted_off_end(999)"

                    

                target_parcel["closedTS"] = ts_m.group(1)
                target_parcel["exitTS"]=ts_m.group(1)
                target_parcel["exit_location"]=location
                if target_parcel["registerTS"]!=None:
                    target_parcel["complete_record"]=True

            elif msg == "ItemDeRegister":
                if target_parcel["date"]==None:
                    target_parcel["date"]=date_part
                target_parcel["closedTS"] = ts_m.group(1)
                target_parcel["exitTS"]=ts_m.group(1)
                target_parcel["exit_location"]=location
                target_parcel["latestTS"]=ts_m.group(1)
                if target_parcel["registerTS"]!=None:
                    target_parcel["complete_record"]=True

                if target_parcel["status"] == "sorted":
                    continue

                exit_state = parts[8].strip()
                if exit_state:
                    target_parcel["exit_state"] = exit_state

                    if exit_state=="1":
                        target_parcel["deregister"]="unexpected"

                    elif exit_state=="2":
                        target_parcel["deregister"]="expected"


                if target_parcel["status"]=="open":
                    if target_parcel["exit_state"]=="1":
                        target_parcel["status"]="track_loss"

                    elif target_parcel["exit_state"]=="2":
                        target_parcel["status"]="track_loss"




    all_parcel_records = [p for p in all_parcel_records if p.get("registerTS") is not None]
    return all_parcel_records
 

def convert_txt_to_json(input_file: str):
    """
    Reads a .txt log file, parses it into JSON, and returns the parsed data.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"File not found: {input_file}")

    with open(input_file, "r", encoding="utf-8") as f:
        log_text = f.read()

    parsed_data = parse_log_lines(log_text)
    return parsed_data




if __name__ == "__main__":
    input_file = input("Enter the log file name (e.g., log.txt): ").strip()
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = base_filename + ".json"

    try:
        with open(input_file, "r", encoding="utf-8") as f:
            log_text = f.read()
        parsed_data = parse_log_lines(log_text)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=4)

        print(f"\n Parsed data saved to '{output_file}'")
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")