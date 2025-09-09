# gui/resource_tab.py

import os
import traceback
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QProgressBar, QSpacerItem, QSizePolicy,
                             QCheckBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal

from gui.widgets import FileSelector, StatusPanel
from gui.utils import show_error, get_default_output_path, open_file, show_question

import pandas as pd

# Import core functionality
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.excel_handler import ExcelHandler
from core.data_processor import DataProcessor


class ResourceSummaryWorker(QThread):
    """Worker thread for resource summary generation to keep UI responsive"""
    progress_update = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str, str)

    def __init__(self, input_file, deployments_file, output_file=None,phases_checked=None,columns=None):
        super().__init__()
        self.input_file = input_file
        self.deployments_file = deployments_file
        self.output_file = output_file
        self.phases_checked = phases_checked
        if columns == []:
            self.columns = None
        else:
            self.columns = columns

    def run(self):
        try:
            # Read the input Excel files
            self.progress_update.emit("Reading data from input file...")
            df = ExcelHandler.read_excel(self.input_file)

            self.progress_update.emit("Reading deployments data...")
            deployments_df = ExcelHandler.read_excel(self.deployments_file)

            # Validate the required columns
            self.progress_update.emit("Validating input data...")
            required_columns = ['Ressource', 'Projet', 'Soumise (h)']
            is_valid, missing_columns = DataProcessor.validate_dataframe(df, required_columns)

            if not is_valid:
                error_msg = f"The following required columns are missing: {missing_columns}"
                self.finished_signal.emit(False, error_msg, "")
                return

            # Fix column names if needed (Resource vs Ressource)
            if 'Ressource' not in df.columns and 'Resource' in df.columns:
                df.rename(columns={'Resource': 'Ressource'}, inplace=True)

            # Create lookup dictionaries
            self.progress_update.emit("Creating lookup tables for project information...")
            connection_dict = DataProcessor.create_connection_dict(deployments_df, 'Niveau de connexion')
            phase_dict = DataProcessor.create_connection_dict(deployments_df, 'Phase du projet')
            montant_dict = DataProcessor.create_connection_dict(deployments_df, 'Montant total (Contrat) (Commande)')

            # Add Dernière Note mapping if present
            derniere_note_dict = {}
            if 'Dernière Note' in deployments_df.columns and 'Nom' in deployments_df.columns:
                derniere_note_dict = deployments_df.set_index('Nom')['Dernière Note'].to_dict()

            # Calculate Charge JH
            self.progress_update.emit("Calculating 'Charge JH'...")
            df = DataProcessor.calculate_charge_jh(df)

            # Create pivot table
            self.progress_update.emit("Creating pivot table...")
            pivot_df = ExcelHandler.create_pivot_table(df, 'Charge JH', ['Ressource', 'Projet'])
            # Add CA column to pivot_df by mapping project to summed CA
            pivot_df['Montant total (Contrat) (Commande)'] = pivot_df['Projet'].map(montant_dict)
            # Add Dernière Note column to pivot_df by mapping project
            if derniere_note_dict:
                pivot_df['Dernière Note'] = pivot_df['Projet'].map(derniere_note_dict)
            if self.columns is None:
                # Calculate Durée (days between today and Date d'affectation)
                if "Date d'affectation" in deployments_df.columns and "Nom" in deployments_df.columns:
                    date_affect_dict = deployments_df.set_index("Nom")["Date d'affectation"].to_dict()
                    today = pd.Timestamp.now().normalize()
                    duree_list = []
                    for project in pivot_df["Projet"]:
                        date_affect = pd.to_datetime(date_affect_dict.get(project, pd.NaT), errors="coerce")
                        if pd.notna(date_affect):
                            duree = (today - date_affect.normalize()).days
                        else:
                            duree = None
                        duree_list.append(duree)
                    pivot_df["Durée"] = duree_list
                else:
                    pivot_df["Durée"] = None
            else:
                pivot_df["Durée"] = None

            # Format the resource summary with theoretical charge
            self.progress_update.emit("Formatting output data and calculating theoretical charges...")
            result_df = DataProcessor.format_resource_summary(pivot_df, connection_dict, phase_dict, montant_dict)

            # Get output file path if not specified
            if not self.output_file:
                self.output_file = get_default_output_path(self.input_file, "_resource_summary")

            # Detect consultant rows (they have a value in "Somme de Charge JH" and NaN in "Charge JH")
            result_df["is_consultant"] = result_df["Charge JH"].isna() & result_df["Somme de Charge JH"].notna()

            # Add a group ID to each consultant block
            result_df["consultant_id"] = result_df["Resource/ PROJET"].where(result_df["is_consultant"]).ffill()

            result_df["__original_order"] = range(len(result_df))
            result_df = result_df[
                result_df["Phase du projet"].isin(self.phases_checked) |
                result_df["Somme de Charge JH"].notna() & (result_df["Somme de Charge JH"] != 0)
                ]
            result_df = result_df.sort_values("__original_order").drop(columns="__original_order")

            # Drop consultant rows temporarily
            projects_only = result_df[~result_df["is_consultant"]].copy()

            # Recalculate sums
            sums = projects_only.groupby("consultant_id")["Charge JH"].sum().reset_index()
            sums.columns = ["consultant_id", "new_sum"]

            # Ensure every consultant has a row in the sums (even if 0)
            all_consultants = result_df[result_df["is_consultant"]][["consultant_id"]].drop_duplicates()
            sums = all_consultants.merge(sums, on="consultant_id", how="left").fillna(0)
            sums = sums.infer_objects(copy=False)

            # Merge back the recalculated sum into the original DataFrame
            result_df = result_df.merge(sums, on="consultant_id", how="left")

            # Update consultant rows only
            result_df.loc[result_df["is_consultant"], "Somme de Charge JH"] = result_df.loc[result_df["is_consultant"], "new_sum"]

            # Add sum of ecart for each consultant
            if 'Ecart' in result_df.columns:
                # Calculate sum of ecart for each consultant
                ecart_sums = result_df.groupby("consultant_id")['Ecart'].sum().reset_index()
                ecart_sums.columns = ["consultant_id", "ecart_sum"]
                
                # Merge back the ecart sum into the original DataFrame
                result_df = result_df.merge(ecart_sums, on="consultant_id", how="left")
                
                # Update consultant rows only with the ecart sum
                result_df.loc[result_df["is_consultant"], "Somme de Ecart"] = result_df.loc[result_df["is_consultant"], "ecart_sum"]
                
                # Clean up the temporary ecart_sum column
                result_df = result_df.drop(columns=["ecart_sum"])
                
                # Move the new column right after the 'Ecart' column
                ecart_index = result_df.columns.get_loc('Ecart')
                # Remove the column from its current position
                new_col = result_df.pop('Somme de Ecart')
                # Insert it right after 'Ecart'
                result_df.insert(ecart_index + 1, 'Somme de Ecart', new_col)

            # Clean up
            result_df = result_df.drop(columns=["is_consultant", "new_sum", "consultant_id"])

            # Add a value to a specific cell (e.g., 'col2' in 'row2')
            #result_df.loc[0, 'somme ecart'] = result_df['Ecart'].sum()

            if self.columns is not None:
                result_df = result_df.drop(columns=self.columns)

            # Write to Excel
            self.progress_update.emit(f"Writing results to '{self.output_file}'...")
            # Create High CA sheet (projects with Montant total (Contrat) (Commande) > 3000)
            if 'Montant total (Contrat) (Commande)' in result_df.columns:
                high_ca_df = result_df[result_df['Montant total (Contrat) (Commande)'] > 3000]
            else:
                high_ca_df = result_df.iloc[0:0].copy()  # empty if column missing
            
            # Create Projects by Month sheet
            self.progress_update.emit("Creating projects by month summary...")
            projects_by_year_df = DataProcessor.create_projects_by_month_summary(deployments_df, self.phases_checked)
            
            # Create Theoretical Charge by Resource sheet
            self.progress_update.emit("Creating theoretical charge by resource summary...")
            theoretical_charge_df = DataProcessor.create_theoretical_charge_by_resource_from_summary(result_df)
            
            ExcelHandler.write_multiple_sheets({
                'Resource Summary': result_df,
                'High CA': high_ca_df,
                'Projets par ans': projects_by_year_df,
                'Charge Théorique par Consultant': theoretical_charge_df
            }, self.output_file)

            # Generate graphs and add to 'graphes' sheet
            import matplotlib.pyplot as plt
            figures = []
            # Bar chart: Charge JH par consultant
            col_proj = 'Resource/ PROJET'
            col_jh = 'Somme de Charge JH'
            # Only use rows where 'Somme de Charge JH' is notna and 'Resource/ PROJET' is not indented
            chart_data = result_df[(result_df[col_jh].notna()) & (~result_df[col_proj].str.startswith('    '))]
            if not chart_data.empty:
                fig1, ax1 = plt.subplots(figsize=(6, 3))
                ax1.bar(chart_data[col_proj].astype(str), chart_data[col_jh])
                ax1.set_xlabel("Consultants")
                ax1.set_ylabel(col_jh)
                ax1.set_title("Charge JH par consultant")
                plt.xticks(rotation=45, ha="right")
                figures.append(fig1)
            
            # Bar chart: Somme Ecart par consultant
            col_somme_ecart = 'Somme de Ecart'
            if col_somme_ecart in result_df.columns:
                # Only use rows where 'Somme de Ecart' is notna and 'Resource/ PROJET' is not indented
                ecart_chart_data = result_df[(result_df[col_somme_ecart].notna()) & (~result_df[col_proj].str.startswith('    '))]
                if not ecart_chart_data.empty:
                    fig3, ax3 = plt.subplots(figsize=(6, 3))
                    bars = ax3.bar(ecart_chart_data[col_proj].astype(str), ecart_chart_data[col_somme_ecart])
                    ax3.set_xlabel("Consultants")
                    ax3.set_ylabel(col_somme_ecart)
                    ax3.set_title("Somme Ecart par consultant")
                    plt.xticks(rotation=45, ha="right")
                    
                    # Color bars based on positive/negative values
                    for bar in bars:
                        if bar.get_height() >= 0:
                            bar.set_color('green')
                        else:
                            bar.set_color('red')
                    
                    figures.append(fig3)
            # Pie chart: Distribution de l'ecarts
            col_ecart = 'Ecart'
            if col_ecart in result_df.columns:
                ecart = result_df[col_ecart].dropna()
                if not ecart.empty:
                    categories = [
                        (ecart > 0).sum(),
                        (ecart < 0).sum(),
                        (ecart == 0).sum(),
                    ]
                    labels = ["Positive", "Negative", "Zero"]
                    fig2, ax2 = plt.subplots()
                    ax2.pie(categories, labels=labels, autopct="%1.1f%%", startangle=90)
                    ax2.set_title("Distribution de l'ecarts")
                    figures.append(fig2)
            
            if figures:
                ecart_sum = result_df['Ecart'].sum() if 'Ecart' in result_df.columns else None
                ExcelHandler.add_graphs_sheet(self.output_file, 'graphes', figures, ecart_sum=ecart_sum)

            self.finished_signal.emit(True, "Resource summary generated successfully!", self.output_file)

        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            traceback_str = traceback.format_exc()
            self.finished_signal.emit(False, f"{error_msg}\n\nDetails:\n{traceback_str}", "")


class ResourceSummaryTab(QWidget):
    """Tab for generating resource summary with theoretical charge"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.worker = None
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(20)

        # Title label
        title_label = QLabel("Générer la consommation de charge")
        title_label.setObjectName("HeaderLabel")
        layout.addWidget(title_label)

        # File selection widgets
        self.input_file_selector = FileSelector("Fichier de consommation:")
        layout.addWidget(self.input_file_selector)

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

        # Select project phases
        phase_layout = QVBoxLayout()
        phase_label = QLabel("Phases:")
        phase_layout.addWidget(phase_label)

        checkbox_layout = QHBoxLayout()

        checkbox_layout1 = QVBoxLayout()
        checkbox_layout2 = QVBoxLayout()
        checkbox_layout3 = QVBoxLayout()
        checkbox_layout4 = QVBoxLayout()

        self.cadrage_checkbox = QCheckBox("Cadrage / spécification")
        self.cadrage_checkbox.setChecked(True)
        checkbox_layout1.addWidget(self.cadrage_checkbox)

        self.developpement_checkbox = QCheckBox("Développement")
        self.developpement_checkbox.setChecked(True)
        checkbox_layout1.addWidget(self.developpement_checkbox)

        self.non_demarre_checkbox = QCheckBox("Non démarré (nouveau projet)")
        self.non_demarre_checkbox.setChecked(True)
        checkbox_layout1.addWidget(self.non_demarre_checkbox)

        checkbox_layout.addLayout(checkbox_layout1)

        self.recette_interne_checkbox = QCheckBox("Recette interne")
        self.recette_interne_checkbox.setChecked(True)
        checkbox_layout2.addWidget(self.recette_interne_checkbox)

        self.recette_user_checkbox = QCheckBox("Recette utilisateur")
        self.recette_user_checkbox.setChecked(True)
        checkbox_layout2.addWidget(self.recette_user_checkbox)

        self.preprod_checkbox = QCheckBox("Pré-production")
        self.preprod_checkbox.setChecked(True)
        checkbox_layout2.addWidget(self.preprod_checkbox)

        checkbox_layout.addLayout(checkbox_layout2)

        self.arrete_checkbox = QCheckBox("Projet arrêté définitivement")
        self.arrete_checkbox.setChecked(False)
        checkbox_layout3.addWidget(self.arrete_checkbox)

        self.pause_checkbox = QCheckBox("Projet en pause")
        self.pause_checkbox.setChecked(False)
        checkbox_layout3.addWidget(self.pause_checkbox)

        self.production_checkbox = QCheckBox("En production (VSR)")
        self.production_checkbox.setChecked(False)
        checkbox_layout3.addWidget(self.production_checkbox)

        checkbox_layout.addLayout(checkbox_layout3)

        self.non_demarre_autre_lot_checkbox = QCheckBox("Non démarré (autre lot)")
        self.non_demarre_autre_lot_checkbox.setChecked(False)
        checkbox_layout4.addWidget(self.non_demarre_autre_lot_checkbox)

        self.termine_checkbox = QCheckBox("Terminé (VSR signée)")
        self.termine_checkbox.setChecked(False)
        checkbox_layout4.addWidget(self.termine_checkbox)



        checkbox_layout.addLayout(checkbox_layout4)

        phase_layout.addLayout(checkbox_layout)

        phase_layout.addStretch()

        layout.addLayout(phase_layout)

        duree_layout = QVBoxLayout()

        self.d_label = QLabel("Colonnes:")
        duree_layout.addWidget(self.d_label)

        self.duree_checkbox = QCheckBox("Durée")
        self.duree_checkbox.setChecked(False)
        duree_layout.addWidget(self.duree_checkbox)

        layout.addLayout(duree_layout)

        # Generate button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.generate_button = QPushButton("Générer")
        self.generate_button.clicked.connect(self.generate_summary)
        button_layout.addWidget(self.generate_button)
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

    def generate_summary(self):
        """Start the process to generate resource summary"""
        input_file = self.input_file_selector.get_file_path()
        deployments_file = self.deployments_file_selector.get_file_path()
        output_file = self.output_file_selector.get_file_path()

        # Validate inputs
        if not input_file:
            show_error(self, "Missing Input", "Please select a resource data file.")
            return

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
        self.worker = ResourceSummaryWorker(input_file, deployments_file, output_file,self.get_checked_phases(),self.get_checked_columns())
        self.worker.progress_update.connect(self.update_progress)
        self.worker.finished_signal.connect(self.on_generation_finished)
        self.worker.start()

    def update_progress(self, message):
        """Update progress status with message"""
        self.status_panel.set_status(message, "info")

    def on_generation_finished(self, success, message, output_file):
        """Handle completion of the generation process"""
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
            print(message)
            self.status_panel.set_status(message, "error")

    def get_checked_phases(self):
        """"
        methode that returns a list containing the phases selected
        """
        phases = [""]
        if self.cadrage_checkbox.isChecked():
            phases.append("Cadrage / Spécifications")
        if self.developpement_checkbox.isChecked():
            phases.append("Développement")
        if self.production_checkbox.isChecked():
            phases.append("En production (VSR)")
        if self.non_demarre_autre_lot_checkbox.isChecked():
            phases.append("Non démarré (autre lot)")
        if self.non_demarre_checkbox.isChecked():
            phases.append("Non démarré (nouveau projet)")
        if self.preprod_checkbox.isChecked():
            phases.append("Pré-production")
        if self.arrete_checkbox.isChecked():
            phases.append("Projet arrêté définitivement")
        if self.pause_checkbox.isChecked():
            phases.append("Projet en pause")
        if self.recette_interne_checkbox.isChecked():
            phases.append("Recette interne")
        if self.recette_user_checkbox.isChecked():
            phases.append("Recette utilisateur")
        if self.termine_checkbox.isChecked():
            phases.append("Terminé (VSR signée)")
        return phases

    def get_checked_columns(self):
        columns=[]
        if not self.duree_checkbox.isChecked():
            columns.append("Durée")
        return columns
