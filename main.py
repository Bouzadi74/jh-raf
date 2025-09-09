import os
import sys
import warnings


warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Add project root to sys.path to allow importing from project modules
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from core.excel_handler import ExcelHandler
from core.data_processor import DataProcessor
from utils.helpers import get_user_file_path, get_default_output_path, get_user_choice


def display_menu():
    """Display main menu options."""
    print("\nExcel Resource Summary Generator")
    print("===============================")
    print("1. Generate Resource Summary with Theoretical Charge and Ecart")
    print("2. Add RAF to Deployments File")
    print("3. Exit")
    return get_user_choice("\nSelect an option (1-3): ", ["1", "2", "3"])


def generate_resource_summary():
    """Generate resource summary with theoretical charge."""
    try:
        # Get input file path from user
        input_file = get_user_file_path("\nEnter the path to your Excel file with resource data: ")

        # Get deployments file path from user
        deployments_file = get_user_file_path("\nEnter the path to your deployments Excel file: ")

        # Read the input Excel files
        print(f"\nReading data from '{input_file}'...")
        df = ExcelHandler.read_excel(input_file) #turns it to a pandas df

        print(f"Reading deployments data from '{deployments_file}'...")
        deployments_df = ExcelHandler.read_excel(deployments_file)

        # Validate the required columns
        print("Validating input data...")
        required_columns = ['Ressource', 'Projet', 'Soumise (h)']
        is_valid, missing_columns = DataProcessor.validate_dataframe(df, required_columns)

        if not is_valid:
            print(f"Error: The following required columns are missing: {missing_columns}")
            print(f"Available columns: {df.columns.tolist()}")
            return

        # Fix column names if needed (Resource vs Ressource)
        if 'Ressource' not in df.columns and 'Resource' in df.columns:
            df.rename(columns={'Resource': 'Ressource'}, inplace=True)

        # Create lookup dictionaries
        print("Creating lookup tables for project information...")
        connection_dict = DataProcessor.create_connection_dict(deployments_df, 'Niveau de connexion')
        phase_dict = DataProcessor.create_connection_dict(deployments_df, 'Phase du projet')
        montant_dict = DataProcessor.create_connection_dict(deployments_df, 'Montant total (Contrat) (Commande)')
        # Add this: create CA dictionary by summing CA by project
        ca_dict = {}
        if 'CA' in deployments_df.columns and 'Nom' in deployments_df.columns:
            ca_by_project = deployments_df.groupby('Nom')['CA'].sum()
            ca_dict = ca_by_project.to_dict()

        # Calculate Charge JH
        print("Calculating 'Charge JH' (Soumise (h) / 8)...")
        df = DataProcessor.calculate_charge_jh(df)

        # Create pivot table
        print("Creating pivot table...")
        pivot_df = ExcelHandler.create_pivot_table(df, 'Charge JH', ['Ressource', 'Projet'])
        # Add CA column to pivot_df by mapping project to summed CA
        pivot_df['CA'] = pivot_df['Projet'].map(ca_dict)

        # Format the resource summary with theoretical charge
        print("Formatting output data and calculating theoretical charges...")
        result_df = DataProcessor.format_resource_summary(pivot_df, connection_dict, phase_dict, montant_dict)

        # Get output file path
        default_output = get_default_output_path(input_file, "_resource_summary")
        output_file = get_user_file_path(
            f"\nEnter the path for the output file (or press Enter for default: {default_output}): ",
            must_exist=False
        )

        if not output_file.strip():
            output_file = default_output

        # Write to Excel
        print(f"\nWriting results to '{output_file}'...")
        ExcelHandler.write_excel(result_df, output_file, 'Resource Summary')

        print(f"\nSuccess! Results saved to {output_file}")

        # Ask if user wants to open the output file
        open_file = input("\nDo you want to open the output file? (y/n): ").lower()
        if open_file.startswith('y'):
            print("Opening output file...")
            ExcelHandler.open_file(output_file)

    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        import traceback
        traceback.print_exc()


def add_raf_to_deployments():
    """Add RAF column to deployments file and create a RAF summary sheet."""
    try:
        # Get deployments file path from user
        deployments_file = get_user_file_path("\nEnter the path to your deployments Excel file: ")

        # Read the deployments Excel file
        print(f"\nReading deployments data from '{deployments_file}'...")
        deployments_df = ExcelHandler.read_excel(deployments_file)

        # Validate the required columns
        print("Validating input data...")
        required_columns = ['Niveau de connexion', 'Phase du projet', 'Date de MEP']
        is_valid, missing_columns = DataProcessor.validate_dataframe(deployments_df, required_columns)

        if not is_valid:
            print(f"Error: The following required columns are missing: {missing_columns}")
            print(f"Available columns: {deployments_df.columns.tolist()}")
            return

        # Calculate RAF
        print("Calculating RAF values based on connection level and project phase...")
        from core.raf_processor import RAFProcessor
        deployments_df = RAFProcessor.calculate_raf(deployments_df)

        # Get output file path
        default_output = get_default_output_path(deployments_file, "_with_raf")
        output_file = get_user_file_path(
            f"\nEnter the path for the output file (or press Enter for default: {default_output}): ",
            must_exist=False
        )

        if not output_file.strip():
            output_file = default_output

        # Write to Excel
        print(f"\nWriting results to '{output_file}'...")

        # Copy the original file first to preserve all formatting and data
        import shutil
        try:
            shutil.copy2(deployments_file, output_file)
        except shutil.SameFileError:
            pass  # Ignore if source and destination are the same

        # Use openpyxl to modify the file
        from openpyxl import load_workbook

        # Load the workbook
        print("Adding RAF column to deployments data...")
        workbook = load_workbook(output_file)

        # Add RAF column
        workbook = RAFProcessor.add_raf_to_workbook(workbook, deployments_df)

        # Create RAF summary sheet
        print("Creating RAF summary sheet with weekly and monthly breakdowns...")
        workbook = RAFProcessor.create_raf_summary_sheet(workbook, deployments_df)

        # Save the workbook
        workbook.save(output_file)

        print(f"\nSuccess! Results saved to {output_file}")

        # Ask if user wants to open the output file
        open_file = input("\nDo you want to open the output file? (y/n): ").lower()
        if open_file.startswith('y'):
            print("Opening output file...")
            ExcelHandler.open_file(output_file)

    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point for the application."""
    while True:
        choice = display_menu()

        if choice == "1":
            generate_resource_summary()
        elif choice == "2":
            add_raf_to_deployments()
        elif choice == "3":
            print("\nExiting application. Goodbye!")
            break

        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()