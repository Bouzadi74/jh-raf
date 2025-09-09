# gui/utils.py

import os
import sys
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtCore import QUrl
from PyQt5.QtGui import QDesktopServices


def show_message(parent, title, message, icon=QMessageBox.Information):
    """Display a message box"""
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    msg_box.setIcon(icon)
    msg_box.setStandardButtons(QMessageBox.Ok)
    msg_box.exec_()


def show_error(parent, title, message):
    """Display an error message box"""
    show_message(parent, title, message, QMessageBox.Critical)


def show_warning(parent, title, message):
    """Display a warning message box"""
    show_message(parent, title, message, QMessageBox.Warning)


def show_question(parent, title, message):
    """Display a question message box with Yes/No buttons"""
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    msg_box.setIcon(QMessageBox.Question)
    msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
    return msg_box.exec_() == QMessageBox.Yes


def get_default_output_path(input_file, suffix="_output"):
    """Generate a default output file path based on an input file"""
    input_dir = os.path.dirname(input_file) or '.'
    input_filename = os.path.basename(input_file)
    name, ext = os.path.splitext(input_filename)

    return os.path.join(input_dir, f"{name}{suffix}{ext}")


def open_file(file_path):
    """Open a file with the default system application"""
    if sys.platform == 'win32':
        os.startfile(file_path)
    else:
        QDesktopServices.openUrl(QUrl.fromLocalFile(file_path))