PowerBI API Scraper

A Python script for scraping data from PowerBI API endpoints with checkpoint support for resumable downloads.


Features
• **Checkpoint System**: Automatically saves progress and can resume from interruption
• **Incremental CSV Writing**: Writes data to CSV file incrementally to handle large datasets
• **Pagination Support**: Handles PowerBI's pagination using restart tokens
• **Data Processing**: Includes custom logic for processing PowerBI's compressed data format
• **Error Handling**: Robust error handling with detailed logging


Installation
1. Clone this repository:

git clone <repository-url>
cd <repository-name>

1. Install required dependencies:

pip install -r requirements.txt


Usage

Basic Usage

Run the scraper with default settings:

python main.py


This will:
• Create `result.csv` with scraped data
• Save progress to `checkpoint.json`
• Resume automatically if interrupted


Command Line Options

python main.py [OPTIONS]


**Options:**
• `--output FILENAME` - Specify output CSV file (default: `result.csv`)
• `--checkpoint FILENAME` - Specify checkpoint file (default: `checkpoint.json`)
• `--delay SECONDS` - Set delay between requests in seconds (default: `1.0`)
• `--fresh` - Start from scratch, ignoring existing checkpoint


Examples

Start a fresh scrape with custom output file:

python main.py --output my_data.csv --fresh


Resume previous scrape with 2-second delay between requests:

python main.py --delay 2.0


Use custom checkpoint file:

python main.py --checkpoint my_checkpoint.json


How It Works

Data Processing Pipeline
1. **API Request**: Sends POST request to PowerBI API with pagination token
2. **Response Processing**: 
- Reconstructs compressed arrays using bitsets
- Expands value dictionaries
- Converts timestamps to readable dates
- Cleans newline characters
3. **CSV Writing**: Writes processed data incrementally to CSV
4. **Checkpoint Saving**: Saves progress after each page
5. **Pagination**: Uses restart tokens to fetch next page


Checkpoint System

The script saves progress after each successful page fetch. If interrupted:
• Run the script again without `--fresh` flag
• It will automatically resume from the last saved checkpoint
• No duplicate data will be written


Data Format

The output CSV contains Ohio permit data with the following columns:
• Permit Number
• DBA Name
• Address
• City
• Postal Code
• Permit Class
• State
• Status
• Tax District Number
• County
• Muni/Township
• Submitted Date
• Alt Address
• End Date
• Issued Date
• Wholesale Store Number
• Is In Safekeeping
• Closing Authority
• Site Vote
• Legacy Permit #
• Location Name
• Original Issue Date
• Permit Holder
• Application Type


Technical Details

PowerBI API Structure

The script handles PowerBI's specific data format:
• **Bitset Compression**: Uses "R" and "\u00d8" bitsets to compress repeated/null values
• **Value Dictionaries**: Replaces numeric indexes with actual string values
• **Restart Tokens**: Manages pagination through PowerBI's token system


Error Handling
• Network errors: Retries with exponential backoff
• Keyboard interrupts: Saves checkpoint before exit
• Data validation: Checks for empty responses and invalid data


Requirements
• Python 3.7+
• requests library


Output Files
• `result.csv` - Main output file with scraped data
• `checkpoint.json` - Progress tracking file (can be deleted to start fresh)


Troubleshooting

**Script stops unexpectedly:**
• Check your internet connection
• Verify the PowerBI API endpoint is accessible
• Review error messages in console output


**Duplicate data in CSV:**
• Use `--fresh` flag to start over
• Delete existing `result.csv` and `checkpoint.json` files


**Slow performance:**
• Increase `--delay` value to reduce request frequency
• Check network bandwidth


License

This project is provided as-is for educational and research purposes.


Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


Disclaimer

This script is designed to work with publicly accessible PowerBI endpoints. Always ensure you have permission to scrape data from any API endpoint and comply with the service's terms of use.
