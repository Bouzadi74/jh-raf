# gui/raf_tab.py

import os
import traceback
import shutil
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QProgressBar, QSpacerItem, QSizePolicy,
                             QCheckBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal

from gui.widgets import FileSelector, StatusPanel
from gui.utils import show_error, get_default_output_path, open_file, show_question

# Import core functionality
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.excel_handler import ExcelHandler
from core.data_processor import DataProcessor
from core.raf_processor import RAFProcessor
from openpyxl import load_workbook


class RAFWorker(QThread):
    """Worker thread for RAF processing to keep UI responsive"""
    progress_update = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str, str)

    def __init__(self, deployments_file, output_file=None):
        super().__init__()
        self.deployments_file = deployments_file
        self.output_file = output_file

    def run(self):
        try:
            # Read the deployments Excel file
            self.progress_update.emit(f"Reading deployments data...")
            deployments_df = ExcelHandler.read_excel(self.deployments_file)

            # Validate the required columns
            self.progress_update.emit("Validating input data...")
            required_columns = ['Niveau de connexion', 'Phase du projet', 'Date de MEP']
            is_valid, missing_columns = DataProcessor.validate_dataframe(deployments_df, required_columns)

            if not is_valid:
                error_msg = f"The following required columns are missing: {missing_columns}"
                self.finished_signal.emit(False, error_msg, "")
                return

            # Calculate RAF
            self.progress_update.emit("Calculating RAF values...")
            deployments_df = RAFProcessor.calculate_raf(deployments_df)

            # Get output file path if not specified
            if not self.output_file:
                self.output_file = get_default_output_path(self.deployments_file, "_with_raf")

            # Copy the original file first to preserve all formatting and data
            self.progress_update.emit("Creating output file...")
            try:
                shutil.copy2(self.deployments_file, self.output_file)
            except shutil.SameFileError:
                pass  # Ignore if source and destination are the same

            # Use openpyxl to modify the file
            self.progress_update.emit("Adding RAF column to deployments data...")
            workbook = load_workbook(self.output_file)

            # Add RAF column
            workbook = RAFProcessor.add_raf_to_workbook(workbook, deployments_df)

            # Create RAF summary sheet
            self.progress_update.emit("Creating RAF summary sheet...")
            workbook = RAFProcessor.create_raf_summary_sheet(workbook, deployments_df)

            # Save the workbook
            self.progress_update.emit("Saving workbook...")
            workbook.save(self.output_file)

            self.finished_signal.emit(True, "RAF calculation completed successfully!", self.output_file)

        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            traceback_str = traceback.format_exc()
            self.finished_signal.emit(False, f"{error_msg}\n\nDetails:\n{traceback_str}", "")


class RAFTab(QWidget):
    """Tab for adding RAF calculations to deployments file"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(20)

        # Title label
        title_label = QLabel("Générer le RAF ?")
        title_label.setObjectName("HeaderLabel")
        layout.addWidget(title_label)

        # File selection widgets
        self.deployments_file_selector = FileSelector("Fichier des déploiements:")
        layout.addWidget(self.deployments_file_selector)

        self.output_file_selector = FileSelector("Fichier de sortie (facultatif):", is_save=True)
        layout.addWidget(self.output_file_selector)

        # Option to open file after generation
        option_layout = QHBoxLayout()
        self.open_after_checkbox = QCheckBox("Ouvrir le fichier après la génération")
        self.open_after_checkbox.setChecked(True)
        option_layout.addWidget(self.open_after_checkbox)
        option_layout.addStretch()
        layout.addLayout(option_layout)

        # Process button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.process_button = QPushButton("Générer")
        self.process_button.clicked.connect(self.process_raf)
        button_layout.addWidget(self.process_button)
        layout.addLayout(button_layout)

        # Status panel
        self.status_panel = StatusPanel()
        layout.addWidget(self.status_panel)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # Add spacer to push everything to the top
        spacer = QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Expanding)
        layout.addItem(spacer)

    def process_raf(self):
        """Start the process to add RAF calculations"""
        deployments_file = self.deployments_file_selector.get_file_path()
        output_file = self.output_file_selector.get_file_path()

        # Validate inputs
        if not deployments_file:
            show_error(self, "Missing Input", "Please select a deployments file.")
            return

        # Disable controls during processing
        self.setEnabled(False)

        # Show progress UI elements
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.status_panel.set_status("Processing...", "info")

        # Start worker thread
        self.worker = RAFWorker(deployments_file, output_file)
        self.worker.progress_update.connect(self.update_progress)
        self.worker.finished_signal.connect(self.on_processing_finished)
        self.worker.start()

    def update_progress(self, message):
        """Update progress status with message"""
        self.status_panel.set_status(message, "info")

    def on_processing_finished(self, success, message, output_file):
        """Handle completion of the RAF processing"""
        # Re-enable controls
        self.setEnabled(True)

        # Update UI
        self.progress_bar.setVisible(False)

        if success:
            self.status_panel.set_status(message, "success")

            # Ask to open the file if checkbox is checked
            if self.open_after_checkbox.isChecked():
                open_file(output_file)
        else:
            self.status_panel.set_status(message, "error")