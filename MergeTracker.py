import pandas as pd
import os
from datetime import datetime
from threading import Lock
from multiprocessing.managers import BaseManager

# MergeTracker for the MergeToStaging.py Table Migration script
# Global tracker dataframe and lock in the main process, with some individual error checking
# The global tracker logs everything with logger, so these errors actually won't be saved unless running on console
tracker_df = None
tracker_lock = None
tracker_manager = None


class TrackerManager(BaseManager):
    pass


class TrackerHandler:
    """Class to handle tracker operations in a multiprocessing-safe way"""

    def __init__(self):
        self.df = pd.DataFrame(columns=['table_name', 'primary_key', 'timestamp'])
        self.lock = Lock()

    def update(self, rows_data):
        """Update the tracker with new rows data"""
        new_rows = pd.DataFrame(rows_data)
        with self.lock:
            if self.df.empty:
                self.df = new_rows
            else:
                self.df = pd.concat([self.df, new_rows], ignore_index=True)
        return len(rows_data)

    def save(self):
        """Save the tracker dataframe to a CSV file"""
        # Create output directory if it doesn't exist
        output_dir = ''
        os.makedirs(output_dir, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/merge_tracker_{timestamp}.csv"

        # Save tracker to CSV
        with self.lock:
            self.df.to_csv(filename, index=False)
            row_count = len(self.df)

        print(f"Merge tracker saved to {filename}")
        print(f"Total rows tracked: {row_count}")
        return filename

    def get_row_count(self):
        """Get the current number of rows in the tracker"""
        with self.lock:
            return len(self.df)


def initialize_tracker():
    """Initialize tracker with multiprocessing support"""
    global tracker_df, tracker_lock, tracker_manager

    # Register our custom manager
    TrackerManager.register('TrackerHandler', TrackerHandler)

    # Create a manager instance
    tracker_manager = TrackerManager()
    tracker_manager.start()

    # Create the shared tracker handler through the manager
    tracker_handler = tracker_manager.TrackerHandler()

    print("Merge tracker initialized with multiprocessing support")
    return tracker_handler


def update_tracker(rows_data, tracker_handler=None):
    """Update the tracker with new rows data"""
    if tracker_handler is None:
        print("Warning: Tracker handler not provided to update_tracker")
        return False

    try:
        count = tracker_handler.update(rows_data)
        return True
    except Exception as e:
        print(f"Error updating tracker: {str(e)}")
        return False


def save_tracker(tracker_handler=None):
    """Save the tracker dataframe to a CSV file"""
    if tracker_handler is None:
        print("Warning: Tracker handler not provided to save_tracker")
        return None

    try:
        filename = tracker_handler.save()
        return filename
    except Exception as e:
        print(f"Error saving tracker: {str(e)}")
        return None