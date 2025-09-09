# gui/main_window.py
import os


from PyQt5.QtWidgets import QMainWindow, QTabWidget, QWidget, QVBoxLayout, QLabel
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon

from gui.resource_tab import ResourceSummaryTab
from gui.raf_tab import RAFTab



class MainWindow(QMainWindow):
    """Main application window with tabs for different operations"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Consommation de charge / RAF")
        self.setMinimumSize(700, 600)

        # Set application icon - try multiple paths
        icon_paths = [
            "icon.ico",  # Current directory
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icon.ico'),  # Project root
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'icon.ico')  # One level up
        ]

        for icon_path in icon_paths:
            if os.path.exists(icon_path):
                self.setWindowIcon(QIcon(icon_path))
                break

        self.setup_ui()

    def setup_ui(self):
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # App title
        title_label = QLabel("Consommation de charge / RAF")
        title_label.setObjectName("TitleLabel")
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)

        # Create tab widget
        tab_widget = QTabWidget()
        main_layout.addWidget(tab_widget)

        # Add tabs
        resource_tab = ResourceSummaryTab()
        tab_widget.addTab(resource_tab, "Consommation de charge")

        raf_tab = RAFTab()
        tab_widget.addTab(raf_tab, "RAF")

        # Set up status bar
        self.statusBar().showMessage("Ready")