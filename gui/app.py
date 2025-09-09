# gui/app.py

import sys
import os

# Add project root to sys.path to allow importing from project modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt

from gui.main_window import MainWindow
from gui.styles import STYLESHEET


def run_app():
    """Run the GUI application"""
    # Create application
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Use Fusion style for consistent cross-platform look

    # Set global stylesheet
    app.setStyleSheet(STYLESHEET)

    # Create and show main window
    window = MainWindow()
    window.show()

    # Run the application
    sys.exit(app.exec_())


if __name__ == "__main__":
    run_app()