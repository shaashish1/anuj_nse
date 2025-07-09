import os
import multiprocessing
import time
import subprocess
from datetime import datetime, time as dt_time

def run_script(script_path):
    """
    Runs a Python script using subprocess and prints detailed process information.
    """
    try:
        print(f"Starting {script_path}...")
        
        # Running the script
        result = subprocess.run(['python', script_path], capture_output=True, text=True)
        
        # Print standard output and errors
        print(f"Output from {script_path}:\n{result.stdout}")
        if result.stderr:
            print(f"Errors from {script_path}:\n{result.stderr}")
        
        result.check_returncode()  # Will raise CalledProcessError if the exit code is non-zero
        print(f"{script_path} executed successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_path}: {e}")
    except Exception as e:
        print(f"Unexpected error while running {script_path}: {e}")

def is_within_time_range(start_time, end_time):
    """
    Checks if the current time is within the specified time range.
    """
    now = datetime.now()
    current_time = now.time()
    return start_time <= current_time <= end_time and now.weekday() < 5  # 0-4 are Monday to Friday

def main():
    scripts_folder = "scripts"  # Path to the folder containing scripts
    print(f"Looking for Python scripts in folder: {scripts_folder}")
    
    # Define the allowed time range
    start_time = dt_time(9, 15)  # 9:15 AM
    end_time = dt_time(15, 40)  # 3:40 PM
    
    while True:
        # Check if the current time is within the allowed range
        if is_within_time_range(start_time, end_time):
            print("Trading session is open. Running scripts...")
            
            scripts = []

            # List all .py files in the scripts folder
            print("Scanning the scripts folder for .py files...")
            for filename in os.listdir(scripts_folder):
                if filename.endswith(".py"):
                    script_path = os.path.join(scripts_folder, filename)
                    scripts.append(script_path)
            
            # Show the scripts found
            if scripts:
                print(f"Found the following scripts to run: {scripts}")
            else:
                print("No .py scripts found in the scripts folder.")
                break  # If no scripts found, break the loop
            
            # Create a process for each script
            print(f"Preparing to run {len(scripts)} script(s)...")
            processes = [multiprocessing.Process(target=run_script, args=(script,)) for script in scripts]

            # Start all processes
            print("Starting all scripts...")
            for process in processes:
                process.start()

            # Wait for all processes to complete
            for process in processes:
                process.join()

            print("All scripts finished running.\n")
            
            # Wait for 50 seconds before rerunning
            print("Waiting 50 seconds before the next execution...")
            time.sleep(50)
        else:
            # If not within working hours, display a message and wait before checking again
            now = datetime.now()
            print(f"It is currently outside of allowed working hours (current time: {now.strftime('%H:%M:%S')}).")
            print("Waiting for the trading session to open...")
            time.sleep(60)  # Check every minute if the session has started

if __name__ == "__main__":
    print("Starting the master process to run all scripts during weekdays from 9:15 AM to 3:40 PM...\n")
    main()
