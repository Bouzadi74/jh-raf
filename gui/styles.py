# gui/styles.py

# Main Colors
PRIMARY_BLUE = "#0099CC"  # Bright blue/teal for primary buttons, highlights
DARK_BLUE = "#1A2833"  # Dark blue/charcoal for headers, navigation
WHITE = "#FFFFFF"  # White for backgrounds, text on dark surfaces
LIGHT_GRAY = "#F7F8FA"  # Light gray for section backgrounds, form fields
MEDIUM_GRAY = "#CCCCCC"  # Medium gray for borders, inactive elements
TEXT_BLACK = "#333333"  # Slightly softened black for main text content

# Accent Colors (for status indicators, etc.)
SUCCESS_GREEN = "#28A745"  # Green for success messages/indicators
WARNING_ORANGE = "#FFC107"  # Orange for warnings
ERROR_RED = "#DC3545"  # Red for errors
INFO_LIGHT_BLUE = "#17A2B8"  # Light blue for information notices

# Transparency variants
OVERLAY_DARK = "rgba(26, 40, 51, 0.7)"  # Semi-transparent dark for modals

# Application Stylesheet
STYLESHEET = f"""
    QMainWindow, QDialog {{
        background-color: {WHITE};
    }}

    QTabWidget::pane {{
        border: 1px solid {MEDIUM_GRAY};
        background-color: {WHITE};
    }}

    QTabBar::tab {{
        background-color: {LIGHT_GRAY};
        color: {TEXT_BLACK};
        padding: 8px 16px;
        border: 1px solid {MEDIUM_GRAY};
        border-bottom: none;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }}

    QTabBar::tab:selected {{
        background-color: {WHITE};
        border-bottom: 1px solid {WHITE};
    }}

    QPushButton {{
        background-color: {PRIMARY_BLUE};
        color: {WHITE};
        border: none;
        border-radius: 4px;
        padding: 8px 16px;
        min-width: 100px;
    }}

    QPushButton:hover {{
        background-color: #007AA3;
    }}

    QPushButton:pressed {{
        background-color: #006080;
    }}

    QPushButton:disabled {{
        background-color: {MEDIUM_GRAY};
        color: #888888;
    }}

    QLabel {{
        color: {TEXT_BLACK};
    }}

    QLineEdit, QComboBox {{
        border: 1px solid {MEDIUM_GRAY};
        border-radius: 4px;
        padding: 6px;
        background-color: {LIGHT_GRAY};
    }}

    QProgressBar {{
        border: 1px solid {MEDIUM_GRAY};
        border-radius: 4px;
        background-color: {LIGHT_GRAY};
        text-align: center;
    }}

    QProgressBar::chunk {{
        background-color: {PRIMARY_BLUE};
        width: 10px;
    }}

    QHeaderView::section {{
        background-color: {DARK_BLUE};
        color: {WHITE};
        padding: 4px;
        border: 1px solid {DARK_BLUE};
    }}

    QStatusBar {{
        background-color: {LIGHT_GRAY};
        color: {TEXT_BLACK};
    }}

    #HeaderLabel {{
        color: {DARK_BLUE};
        font-size: 16px;
        font-weight: bold;
    }}

    #TitleLabel {{
        color: {DARK_BLUE};
        font-size: 20px;
        font-weight: bold;
    }}
"""