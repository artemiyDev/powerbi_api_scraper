
import requests
import json
import csv
import os
from typing import List, Dict, Optional
from datetime import datetime
import time


class PowerBIParserFinal:
    def __init__(self, output_csv: str = "result.csv", checkpoint_file: str = "checkpoint.json"):
        self.base_url = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'ActivityId': '0442498d-f0ef-dddd-aede-067f2528e0e5',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://app.powerbigov.us',
            'Referer': 'https://app.powerbigov.us/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-PowerBI-ResourceKey': 'bedd740d-2544-405d-b74b-578d6f1c4674',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.output_csv = output_csv
        self.checkpoint_file = checkpoint_file

    def load_checkpoint(self) -> tuple[Optional[List], int]:
        """Load checkpoint from file"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"\u2713 Checkpoint loaded: {data['records_processed']} records")
                    return data.get('last_token'), data.get('records_processed', 0)
            except Exception as e:
                print(f"\u26a0\ufe0f Error loading checkpoint: {e}")
        return None, 0

    def save_checkpoint(self, token: Optional[List], count: int):
        """Save checkpoint to file"""
        try:
            checkpoint = {
                'last_token': token,
                'records_processed': count,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"\u26a0\ufe0f Error saving checkpoint: {e}")

    def get_base_payload(self, restart_tokens: Optional[List] = None) -> Dict:
        """Create base payload for API request"""
        payload = {
            "version": "1.0.0",
            "queries": [{
                "Query": {
                    "Commands": [{
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {"Name": "d", "Entity": "Account", "Type": 0},
                                    {"Name": "p", "Entity": "Permit Class Type", "Type": 0},
                                    {"Name": "p1", "Entity": "Permit Transaction", "Type": 0},
                                    {"Name": "p2", "Entity": "Permit Header", "Type": 0},
                                    {"Name": "p3", "Entity": "Permit Status", "Type": 0},
                                    {"Name": "t", "Entity": "Taxing District", "Type": 0},
                                    {"Name": "p4", "Entity": "Parent Account", "Type": 0},
                                    {"Name": "p5", "Entity": "Permit Submission Type", "Type": 0}
                                ],
                                "Select": [
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Account Number"}, "Name": "Account.Account Number",
                                     "NativeReferenceName": "Permit Number"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "DBA Name"},
                                     "Name": "Account.DBA Name", "NativeReferenceName": "DBA Name1"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Address 1 Street 1 2"},
                                     "Name": "Account.Address 1 Street 1 2", "NativeReferenceName": "Address"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Address 1 City"}, "Name": "Account.Address 1 City",
                                     "NativeReferenceName": "City1"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Address 1 PostalCode"},
                                     "Name": "Account.Address 1 PostalCode", "NativeReferenceName": "Postal Code"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p"}},
                                                "Property": "Permit Class"}, "Name": "Permit Class Type.Permit Class",
                                     "NativeReferenceName": "Permit Class"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Address 1 StateorProvince"},
                                     "Name": "Account.Address 1 StateorProvince", "NativeReferenceName": "State"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p3"}},
                                                "Property": "Public Status"}, "Name": "Permit Status.Public Status",
                                     "NativeReferenceName": "Status"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                                                "Property": "Tax District Number"},
                                     "Name": "Taxing District.Tax District Number",
                                     "NativeReferenceName": "Tax District Number"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "County"},
                                     "Name": "Taxing District.County", "NativeReferenceName": "County"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                                                "Property": "Muni/Township"}, "Name": "Taxing District.Muni/Township",
                                     "NativeReferenceName": "Muni/Township"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p1"}},
                                                "Property": "Submitted Date"},
                                     "Name": "Permit Transaction.Submitted Date",
                                     "NativeReferenceName": "Submitted Date"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Address 2 FullAddress"},
                                     "Name": "Account.Address 2 FullAddress", "NativeReferenceName": "Alt Address"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p1"}}, "Property": "End Date"},
                                     "Name": "Permit Transaction.End Date", "NativeReferenceName": "End Date"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p1"}},
                                                "Property": "Issue Date"}, "Name": "Permit Transaction.Issue Date",
                                     "NativeReferenceName": "Issued Date"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Wholesale Store Number"},
                                     "Name": "Account.Wholesale Store Number",
                                     "NativeReferenceName": "Wholesale Store Number"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Is In Safekeeping"}, "Name": "Account.Is In Safekeeping",
                                     "NativeReferenceName": "Is In Safekeeping"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Is Intemporary Closing Authority"},
                                     "Name": "Account.Is Intemporary Closing Authority",
                                     "NativeReferenceName": "Closing Authority"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p2"}}, "Property": "Site Vote"},
                                     "Name": "Permit Header.Site Vote", "NativeReferenceName": "Site Vote"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Legacy Permit Number"},
                                     "Name": "Account.Legacy Permit Number", "NativeReferenceName": "Legacy Permit #"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                "Property": "Account Name"}, "Name": "Account.Account Name",
                                     "NativeReferenceName": "Location Name"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p1"}},
                                                "Property": "Original Issue Date"},
                                     "Name": "Permit Transaction.Original Issue Date",
                                     "NativeReferenceName": "Original Issue Date"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p4"}},
                                                "Property": "Account Name"}, "Name": "Parent Account.Account Name",
                                     "NativeReferenceName": "Permit Holder"},
                                    {"Column": {"Expression": {"SourceRef": {"Source": "p5"}},
                                                "Property": "Submission Type Template"},
                                     "Name": "Permit Submission Type.Submission Type Template",
                                     "NativeReferenceName": "Application Type"}
                                ],
                                "Where": [
                                    {"Condition": {"Not": {"Expression": {"In": {"Expressions": [{"Column": {
                                        "Expression": {"SourceRef": {"Source": "p3"}}, "Property": "Public Status"}}],
                                                                                 "Values": [
                                                                                     [{"Literal": {"Value": "null"}}],
                                                                                     [{"Literal": {
                                                                                         "Value": "'Ignore'"}}]]}}}}},
                                    {"Condition": {"Not": {"Expression": {"Comparison": {"ComparisonKind": 0, "Left": {
                                        "Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                                   "Property": "Account Number"}}, "Right": {
                                        "Literal": {"Value": "null"}}}}}}},
                                    {"Condition": {"In": {"Expressions": [{"Column": {
                                        "Expression": {"SourceRef": {"Source": "d"}},
                                        "Property": "Location Permit Group"}}], "Values": [
                                        [{"Literal": {"Value": "'Retailer/Restaurant/Bar'"}}],
                                        [{"Literal": {"Value": "'Temporary Permits'"}}]]}}}
                                ],
                                "OrderBy": [{"Direction": 1, "Expression": {
                                    "Column": {"Expression": {"SourceRef": {"Source": "d"}},
                                               "Property": "Account Number"}}}]
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [{"Projections": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                                                   16, 17, 18, 19, 20, 21, 22, 23], "Subtotal": 1}]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}}
                                },
                                "Version": 1
                            },
                            "ExecutionMetricsKind": 1
                        }
                    }],
                    "QueryId": ""
                }
            }],
            "cancelQueries": [],
            "modelId": 1603692
        }

        if restart_tokens:
            payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]["DataReduction"][
                "Primary"]["Window"]["RestartTokens"] = [restart_tokens]

        return payload

    # ==================== JSON PROCESSING FUNCTIONS ====================

    def is_bit_set_for_index(self, index, bitset):
        """Check if bit is set at given index"""
        return (bitset >> index) & 1 == 1

    def reconstruct_arrays(self, columns_types, dm0):
        """
        Reconstruct arrays by applying bitsets:
        - "R" bitset: copy previous values
        - "\u00d8" bitset: set null values
        """
        length = len(columns_types)
        prevItem = None

        for item in dm0:
            currentItem = item["C"]
            if "R" in item or "\u00d8" in item:
                copyBitset = item.get("R", 0)
                deleteBitSet = item.get("\u00d8", 0)
                for i in range(length):
                    if self.is_bit_set_for_index(i, copyBitset):
                        currentItem.insert(i, prevItem[i])
                    elif self.is_bit_set_for_index(i, deleteBitSet):
                        currentItem.insert(i, None)
            prevItem = currentItem

    def expand_values(self, columns_types, dm0, value_dicts):
        """Substitute indexes with actual values from value dictionaries"""
        for (idx, col) in enumerate(columns_types):
            if "DN" in col:
                for item in dm0:
                    dataItem = item["C"]
                    if isinstance(dataItem[idx], int):
                        valDict = value_dicts[col["DN"]]
                        dataItem[idx] = valDict[dataItem[idx]]

    def replace_newlines_with(self, dm0, replacement=""):
        """Replace newline characters in string values"""
        for item in dm0:
            elem = item["C"]
            for i in range(len(elem)):
                if isinstance(elem[i], str):
                    elem[i] = elem[i].replace("\
", replacement)

    def convert_timestamps_to_dates(self, dm0):
        """Convert Unix timestamps to MM.dd.yyyy format"""
        for item in dm0:
            elem = item["C"]
            for i in range(len(elem)):
                # Check if value is a number (timestamp)
                if isinstance(elem[i], (int, float)) and elem[i] > 100000000000:
                    try:
                        timestamp_seconds = elem[i] / 1000
                        date_obj = datetime.fromtimestamp(timestamp_seconds)
                        elem[i] = date_obj.strftime("%m.%d.%Y")
                    except:
                        pass
                # Handle ISO format dates
                elif isinstance(elem[i], str) and "T" in elem[i] and "-" in elem[i]:
                    try:
                        date_obj = datetime.fromisoformat(elem[i].replace("Z", "+00:00"))
                        elem[i] = date_obj.strftime("%m.%d.%Y")
                    except:
                        pass

    # ==================== RESPONSE PROCESSING ====================

    def process_response(self, response_data: Dict) -> tuple[Optional[List], List[List], Optional[List]]:
        """
        Process API response using custom logic
        Returns: (columns, rows, restart_token)
        """
        data = response_data["results"][0]["result"]["data"]
        dm0 = data["dsr"]["DS"][0]["PH"][0]["DM0"]
        columns_types = dm0[0]["S"]

        # Extract column names
        columns = list(map(
            lambda item: item["GroupKeys"][0]["Source"]["Property"] if item.get("Kind") == 1 else item.get("Value", ""),
            data["descriptor"]["Select"]
        ))

        value_dicts = data["dsr"]["DS"][0].get("ValueDicts", {})

        # Apply processing functions
        self.reconstruct_arrays(columns_types, dm0)
        self.expand_values(columns_types, dm0, value_dicts)
        self.replace_newlines_with(dm0, "")
        self.convert_timestamps_to_dates(dm0)

        # Extract rows
        rows = [item["C"] for item in dm0]

        # Get restart token from RT (Restart Token)
        token = None
        try:
            token = data["dsr"]["DS"][0]["RT"][0]
        except KeyError:
            token = None

        return columns, rows, token

    # ==================== MAIN LOOP ====================

    def fetch_all_data(self, delay: float = 1.0, resume: bool = True) -> int:
        """
        Fetch all data with pagination and write incrementally to CSV
        """
        # Load checkpoint
        restart_token, total_records = self.load_checkpoint() if resume else (None, 0)

        is_resume = total_records > 0
        csv_mode = 'a' if is_resume else 'w'

        print(f"Mode: {'Resume' if is_resume else 'New'}")
        print(f"File: {self.output_csv}")
        print("-" * 60)

        # Open CSV file
        csv_file = open(self.output_csv, csv_mode, newline='', encoding='utf-8')
        writer = None
        seen_permits = set()  # Duplicate protection

        page = 1

        try:
            while True:
                print(f"Page {page}... ", end='', flush=True)

                # Make request
                payload = self.get_base_payload(restart_token)
                response = self.session.post(
                    self.base_url,
                    json=payload,
                    timeout=30
                )

                if response.status_code != 200:
                    print(f"\u274c Error: {response.status_code}")
                    break

                response_data = response.json()
                columns, rows, token = self.process_response(response_data)

                if columns is None or not rows:
                    print("End of data")
                    break

                # Initialize writer on first page
                if writer is None:
                    writer = csv.writer(csv_file)
                    # Write header only for new file
                    if not is_resume:
                        writer.writerow(columns)

                # Write rows with duplicate check
                written = 0
                for row in rows:
                    writer.writerow(row)
                    written += 1

                csv_file.flush()  # Save to disk immediately

                total_records += written
                print(f"{written} records (Total: {total_records})")

                # Save checkpoint
                self.save_checkpoint(token, total_records)

                # Last page?
                if len(rows) < 500:
                    print("\u2705 Last page!")
                    break

                restart_token = token
                page += 1
                time.sleep(delay)

        except KeyboardInterrupt:
            print("\
\
\u26a0\ufe0f  Interrupted by user")
            print(f"Checkpoint saved: {total_records} records")
        except Exception as e:
            print(f"\
\u274c Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            csv_file.close()

        print("-" * 60)
        print(f"\u2713 Completed: {total_records} records")
        print(f"\u2713 File: {self.output_csv}")

        return total_records


def main():
    import argparse

    parser = argparse.ArgumentParser(description='PowerBI Parser with checkpoint support')
    parser.add_argument('--output', default='result.csv', help='Output CSV file')
    parser.add_argument('--checkpoint', default='checkpoint.json', help='Checkpoint file')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests (seconds)')
    parser.add_argument('--fresh', action='store_true', help='Start from scratch')

    args = parser.parse_args()

    # If fresh - remove old files
    if args.fresh:
        for f in [args.checkpoint, args.output]:
            if os.path.exists(f):
                os.remove(f)
                print(f"\ud83d\uddd1\ufe0f  Removed: {f}")

    print("=" * 60)
    print("PowerBI Parser - Final Version")
    print("=" * 60)

    parser_obj = PowerBIParserFinal(
        output_csv=args.output,
        checkpoint_file=args.checkpoint
    )

    start_time = time.time()
    total = parser_obj.fetch_all_data(delay=args.delay, resume=not args.fresh)
    elapsed = time.time() - start_time

    print("\
" + "=" * 60)
    print(f"\u2705 COMPLETED")
    print(f"\ud83d\udcca Total records: {total}")
    print(f"\u23f1\ufe0f  Time: {elapsed:.1f} sec ({elapsed / 60:.1f} min)")
    print(f"\ud83d\udcc1 File: {args.output}")
    print("=" * 60)


if __name__ == "__main__":
    main()
