# gui/widgets.py

from PyQt5.QtWidgets import (QWidget, QLabel, QPushButton, QFileDialog,
                             QHBoxLayout, QVBoxLayout, QFrame)
from PyQt5.QtCore import Qt, pyqtSignal


class FileSelector(QWidget):
    """
    A widget for selecting a file with browse button and display of selected path
    """
    fileSelected = pyqtSignal(str)  # Signal emitted when file is selected

    def __init__(self, label_text, file_type="Excel Files (*.xlsx *.xls)",
                 is_save=False, parent=None):
        super().__init__(parent)
        self.file_type = file_type
        self.is_save = is_save

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Label
        self.label = QLabel(label_text)
        self.label.setMinimumWidth(150)
        layout.addWidget(self.label)

        # Path display
        self.path_label = QLabel("No file selected")
        self.path_label.setStyleSheet(
            "background-color: #F7F8FA; padding: 6px; border: 1px solid #CCCCCC; border-radius: 4px;")
        self.path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(self.path_label, 1)  # 1 = stretch factor

        # Browse button
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        layout.addWidget(self.browse_button)

    def browse_file(self):
        """Open file dialog and update path label"""
        if self.is_save:
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Save File As", "", self.file_type
            )
        else:
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Select File", "", self.file_type
            )

        if file_path:
            self.path_label.setText(file_path)
            self.fileSelected.emit(file_path)

    def get_file_path(self):
        """Return the currently selected file path"""
        path = self.path_label.text()
        return path if path != "No file selected" else ""

    def set_file_path(self, path):
        """Set the file path programmatically"""
        self.path_label.setText(path)


class StatusPanel(QFrame):
    """
    A panel for displaying status messages with appropriate colors
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Sunken)
        self.setStyleSheet("background-color: #F7F8FA; border-radius: 4px; padding: 8px;")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        self.status_label = QLabel("Ready")
        self.status_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        layout.addWidget(self.status_label)

    def set_status(self, message, status_type="normal"):
        """Set status message with appropriate styling"""
        self.status_label.setText(message)

        if status_type == "success":
            self.setStyleSheet("background-color: #d4edda; color: #155724; border-radius: 4px; padding: 8px;")
        elif status_type == "error":
            self.setStyleSheet("background-color: #f8d7da; color: #721c24; border-radius: 4px; padding: 8px;")
        elif status_type == "warning":
            self.setStyleSheet("background-color: #fff3cd; color: #856404; border-radius: 4px; padding: 8px;")
        elif status_type == "info":
            self.setStyleSheet("background-color: #d1ecf1; color: #0c5460; border-radius: 4px; padding: 8px;")
        else:
            self.setStyleSheet("background-color: #F7F8FA; color: #333333; border-radius: 4px; padding: 8px;")