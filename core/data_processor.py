import pandas as pd
from config.rules import get_theoretical_charge


class DataProcessor:
    """
    Processes data for resource summary reports.
    """

    @staticmethod
    def validate_dataframe(df, required_columns):
        """
        Validate that a DataFrame has the required columns.

        Args:
            df (pandas.DataFrame): The DataFrame to validate
            required_columns (list): List of required column names

        Returns:
            tuple: (bool, list) - Success status and list of missing columns
        """
        available_columns = df.columns.tolist()
        missing_columns = [col for col in required_columns if col not in available_columns]

        return len(missing_columns) == 0, missing_columns 

    @staticmethod
    def create_connection_dict(deployments_df, column_name):
        """
        Create a lookup dictionary for a specific column by project name.

        Args:
            deployments_df (pandas.DataFrame): The deployments DataFrame
            column_name (str): The column to extract values from

        Returns:
            dict: Mapping of project names to column values
        """
        result_dict = {}

        if column_name in deployments_df.columns:
            for _, row in deployments_df.iterrows():
                project_name = row['Nom']
                if not pd.isna(row.get(column_name, pd.NA)):
                    result_dict[project_name] = row[column_name]

        return result_dict

    @staticmethod
    def calculate_charge_jh(df):
        """
        Calculate Charge JH (Soumise / 8).

        Args:
            df (pandas.DataFrame): The DataFrame with Soumise (h) column

        Returns:
            pandas.DataFrame: DataFrame with added Charge JH column
        """
        df_copy = df.copy()
        df_copy['Charge JH'] = df_copy['Soumise (h)'] / 8
        return df_copy

    @staticmethod
    def normalize_connection_level(connection_level):
        """
        Normalize connection level to match THEORETICAL_CHARGE_RULES keys.
        
        Args:
            connection_level (str): The raw connection level
            
        Returns:
            str: The normalized connection level
        """
        if not connection_level:
            return None
            
        # Map common variations to standard keys
        mapping = {
            "Connexion Recette int": "Connexion EDI",
            "Connexion Pré-produ": "Connexion EDI", 
            "Connexion Développe": "Connexion EDI",
            "Connexion Recette": "Connexion EDI",
            "Connexion Production": "Connexion EDI"
        }
        
        return mapping.get(connection_level, connection_level)
    
    @staticmethod
    def normalize_project_phase(project_phase):
        """
        Normalize project phase to match THEORETICAL_CHARGE_RULES keys.
        
        Args:
            project_phase (str): The raw project phase
            
        Returns:
            str: The normalized project phase
        """
        if not project_phase:
            return None
            
        # Map common variations to standard keys
        mapping = {
            "Connexion Recette int": "Recette interne",
            "Connexion Pré-produ": "Pré-production",
            "Connexion Développe": "Développement",
            "Connexion Recette": "Recette utilisateur",
            "Connexion Production": "En production (VSR)"
        }
        
        return mapping.get(project_phase, project_phase)

    @staticmethod
    def calculate_theoretical_charge(connection_level, project_phase):
        """
        Calculate the theoretical charge based on connection level and project phase.

        Args:
            connection_level (str): The connection level
            project_phase (str): The project phase

        Returns:
            float: The theoretical charge value
        """
        # Normalize the inputs
        normalized_connection = DataProcessor.normalize_connection_level(connection_level)
        normalized_phase = DataProcessor.normalize_project_phase(project_phase)
        
        return get_theoretical_charge(normalized_connection, normalized_phase)

    @staticmethod
    def create_projects_by_month_summary(deployments_df, phases_checked=None):
        """
        Create a summary of projects by year based on 'Date d'affectation' column.
        If 'Date d'affectation' is empty, use 'Date de création' as fallback.
        Each project will be on a separate row with its "Dernière Note".
        Year appears only once per year group, with a count column.
        
        Args:
            deployments_df (pandas.DataFrame): The deployments DataFrame
            phases_checked (list): List of phases selected by the user
            
        Returns:
            pandas.DataFrame: DataFrame with year, count, project name, and dernière note columns
        """
        if 'Date d\'affectation' not in deployments_df.columns:
            return pd.DataFrame(columns=['Année', 'Nombre de projets', 'Nom du projet', 'Dernière Note'])
        
        # Use phases selected by user, or default to all phases if none provided
        if phases_checked is None or len(phases_checked) == 0:
            allowed_phases = [
                "Cadrage / Spécifications",
                "Recette interne",
                "Développement",
                "Recette utilisateur",
                "Non démarré (nouveau projet)",
                "Pré-production",
            ]
        else:
            allowed_phases = phases_checked
        
        # Create a copy and filter by allowed phases first
        columns_needed = ['Nom', 'Date d\'affectation', 'Phase du projet']
        if 'Date de création' in deployments_df.columns:
            columns_needed.append('Date de création')
        if 'Dernière Note' in deployments_df.columns:
            columns_needed.append('Dernière Note')
        
        df_copy = deployments_df[columns_needed].copy()
        df_copy = df_copy[df_copy['Phase du projet'].isin(allowed_phases)]
        
        # Create a date column that uses Date d'affectation if available, otherwise Date de création
        df_copy['Date effective'] = df_copy['Date d\'affectation']
        
        # If Date de création exists, use it as fallback for empty Date d'affectation
        if 'Date de création' in df_copy.columns:
            # Fill empty Date d'affectation with Date de création
            mask = df_copy['Date d\'affectation'].isna() & df_copy['Date de création'].notna()
            df_copy.loc[mask, 'Date effective'] = df_copy.loc[mask, 'Date de création']
        
        # Filter out rows with missing effective dates
        df_copy = df_copy.dropna(subset=['Date effective'])
        
        if df_copy.empty:
            return pd.DataFrame(columns=['Année', 'Nombre de projets', 'Nom du projet', 'Dernière Note'])
        
        # Convert dates to datetime
        df_copy['Date effective'] = pd.to_datetime(df_copy['Date effective'], errors='coerce')
        df_copy = df_copy.dropna(subset=['Date effective'])
        
        if df_copy.empty:
            return pd.DataFrame(columns=['Année', 'Nombre de projets', 'Nom du projet', 'Dernière Note'])
        
        # Extract year
        df_copy['Année'] = df_copy['Date effective'].dt.year
        
        # Select only the columns we need and rename them
        result_df = df_copy[['Année', 'Nom', 'Dernière Note']].copy()
        result_df.columns = ['Année', 'Nom du projet', 'Dernière Note']
        
        # Sort by year and project name
        result_df = result_df.sort_values(['Année', 'Nom du projet'])
        
        # Add count column for each year
        result_df['Nombre de projets'] = result_df.groupby('Année')['Année'].transform('count')
        
        # Clear project names and notes for 2024 and 2025
        mask_2024_2025 = result_df['Année'].isin([2024, 2025])
        result_df.loc[mask_2024_2025, 'Nom du projet'] = ''
        result_df.loc[mask_2024_2025, 'Dernière Note'] = ''
        
        # Clear year and count for rows after the first one in each year group
        for year in result_df['Année'].unique():
            year_mask = result_df['Année'] == year
            year_indices = result_df[year_mask].index
            if len(year_indices) > 1:
                # Keep first row with year and count, clear others
                result_df.loc[year_indices[1:], 'Année'] = ''
                result_df.loc[year_indices[1:], 'Nombre de projets'] = ''
        
        # Reorder columns
        result_df = result_df[['Année', 'Nombre de projets', 'Nom du projet', 'Dernière Note']]
        
        return result_df

    # @staticmethod
    # def format_resource_summary(pivot_df, connection_dict, phase_dict):
    #     """
    #     Format the resource summary with hierarchical structure.
    #
    #     Args:
    #         pivot_df (pandas.DataFrame): The pivot table DataFrame
    #         connection_dict (dict): Dictionary of connection levels by project
    #         phase_dict (dict): Dictionary of project phases by project
    #
    #     Returns:
    #         pandas.DataFrame: The formatted resource summary
    #     """
    #     # Create output DataFrame with all required columns
    #     result_df = pd.DataFrame(columns=[
    #         'Resource/ PROJET', 'Charge JH', 'Somme de Charge JH',
    #         'Niveau de connexion', 'Phase du projet', 'Charge Theorique'
    #     ])
    #
    #     current_resource = None
    #     row_index = 0
    #
    #     # Sort by resource first, then by project
    #     pivot_df = pivot_df.sort_values(['Ressource', 'Projet'])
    #
    #     for _, row in pivot_df.iterrows():
    #         resource = row['Ressource']
    #         project = row['Projet']
    #         charge = row['Charge JH']
    #
    #         # If this is a new resource, add the resource row
    #         if resource != current_resource:
    #             result_df.loc[row_index, 'Resource/ PROJET'] = resource
    #             resource_charge = pivot_df[pivot_df['Ressource'] == resource]['Charge JH'].sum()
    #             result_df.loc[row_index, 'Somme de Charge JH'] = resource_charge
    #             row_index += 1
    #             current_resource = resource
    #
    #         # Look up connection level and project phase for this project
    #         connection_level = connection_dict.get(project, '')
    #         project_phase = phase_dict.get(project, '')
    #
    #         # Calculate theoretical charge if both values are available
    #         theoretical_charge = None
    #         if connection_level and project_phase:
    #             theoretical_charge = DataProcessor.calculate_theoretical_charge(connection_level, project_phase)
    #
    #         # Add the project row indented under the resource
    #         result_df.loc[row_index, 'Resource/ PROJET'] = f"    {project}"
    #         result_df.loc[row_index, 'Charge JH'] = charge
    #         result_df.loc[row_index, 'Niveau de connexion'] = connection_level
    #         result_df.loc[row_index, 'Phase du projet'] = project_phase
    #
    #         if theoretical_charge is not None:
    #             result_df.loc[row_index, 'Charge Theorique'] = theoretical_charge
    #
    #         row_index += 1
    #
    #     return result_df
    @staticmethod
    def format_resource_summary(pivot_df, connection_dict, phase_dict, montant_dict):
        """
        Format the resource summary with hierarchical structure.

        Args:
            pivot_df (pandas.DataFrame): The pivot table DataFrame
            connection_dict (dict): Dictionary of connection levels by project
            phase_dict (dict): Dictionary of project phases by project
            montant_dict (dict): Dictionary of montant total by project

        Returns:
            pandas.DataFrame: The formatted resource summary
        """
        # Create output DataFrame with all required columns
        result_df = pd.DataFrame(columns=[
            'Resource/ PROJET', 'Charge JH', 'Somme de Charge JH',
            'Niveau de connexion', 'Phase du projet', 'Charge Theorique', 'Ecart','Montant total (Contrat) (Commande)','Dernière Note','Durée'
        ])

        current_resource = None
        row_index = 0

        # Sort by resource first, then by project
        pivot_df = pivot_df.sort_values(['Ressource', 'Projet'])

        # First pass: calculate all ecarts and store them
        resource_ecarts = {}
        
        for _, row in pivot_df.iterrows():
            resource = row['Ressource']
            project = row['Projet']
            charge = row['Charge JH']
            
            # Look up connection level and project phase for this project
            connection_level = connection_dict.get(project, '')
            project_phase = phase_dict.get(project, '')
            
            # Calculate theoretical charge if both values are available
            theoretical_charge = None
            if connection_level and project_phase:
                theoretical_charge = DataProcessor.calculate_theoretical_charge(connection_level, project_phase)
            
            # Calculate ecart if theoretical charge is available
            if theoretical_charge is not None:
                ecart = theoretical_charge - charge
                if resource not in resource_ecarts:
                    resource_ecarts[resource] = []
                resource_ecarts[resource].append(ecart)
        
        # Second pass: build the result DataFrame
        for _, row in pivot_df.iterrows():
            resource = row['Ressource']
            project = row['Projet']
            charge = row['Charge JH']
            ca = row['Montant total (Contrat) (Commande)']
            dn = row ['Dernière Note']
            du = row['Durée']

            # If this is a new resource, add the resource row
            if resource != current_resource:
                result_df.loc[row_index, 'Resource/ PROJET'] = resource
                resource_charge = pivot_df[pivot_df['Ressource'] == resource]['Charge JH'].sum()
                result_df.loc[row_index, 'Somme de Charge JH'] = resource_charge
                
                # Calculate sum of ecarts for this resource
                if resource in resource_ecarts:
                    ecart_sum = sum(resource_ecarts[resource])
                    # Removed: result_df.loc[row_index, 'Somme des Ecarts'] = ecart_sum
                
                row_index += 1
                current_resource = resource

            # Look up connection level and project phase for this project
            connection_level = connection_dict.get(project, '')
            project_phase = phase_dict.get(project, '')

            # Calculate theoretical charge if both values are available
            theoretical_charge = None
            if connection_level and project_phase:
                theoretical_charge = DataProcessor.calculate_theoretical_charge(connection_level, project_phase)

            # Add the project row indented under the resource
            result_df.loc[row_index, 'Resource/ PROJET'] = f"    {project}"
            result_df.loc[row_index, 'Charge JH'] = charge
            result_df.loc[row_index, 'Niveau de connexion'] = connection_level
            result_df.loc[row_index, 'Phase du projet'] = project_phase
            result_df.loc[row_index, 'Montant total (Contrat) (Commande)'] = ca
            result_df.loc[row_index, 'Dernière Note'] = dn
            result_df.loc[row_index, 'Durée'] = du

            if theoretical_charge is not None:
                result_df.loc[row_index, 'Charge Theorique'] = theoretical_charge
                # Calculate Ecart (Charge Theorique - Charge JH)
                ecart = theoretical_charge - charge
                result_df.loc[row_index, 'Ecart'] = ecart

            row_index += 1



        return result_df

    @staticmethod
    def create_theoretical_charge_by_resource_from_summary(resource_summary_df):
        """
        Create a summary of theoretical charges by resource from the Resource Summary sheet.
        Analyzes the Resource Summary DataFrame to count projects and sum theoretical charges per resource.
        
        Args:
            resource_summary_df (pandas.DataFrame): The Resource Summary DataFrame
            
        Returns:
            pandas.DataFrame: DataFrame with resource (person), theoretical charge sum, and project count
        """
        if resource_summary_df.empty:
            return pd.DataFrame(columns=['Ressource', 'Somme Charge Théorique', 'Nombre de projets'])
        
        # Create result DataFrame
        result_data = []
        
        current_resource = None
        current_projects = []
        current_theoretical_charges = []
        
        for _, row in resource_summary_df.iterrows():
            resource_projet = str(row['Resource/ PROJET']) if pd.notna(row['Resource/ PROJET']) else ""
            
            # Check if this is a resource row (not indented, no leading spaces)
            if not resource_projet.startswith('    ') and resource_projet.strip():
                # If we have a previous resource, save their data
                if current_resource is not None:
                    theoretical_sum = sum([charge for charge in current_theoretical_charges if pd.notna(charge) and charge != 0])
                    project_count = len(current_projects)
                    result_data.append({
                        'Ressource': current_resource,
                        'Somme Charge Théorique': theoretical_sum,
                        'Nombre de projets': project_count
                    })
                
                # Start new resource
                current_resource = resource_projet.strip()
                current_projects = []
                current_theoretical_charges = []
            
            # If this is a project row (indented with spaces)
            elif resource_projet.startswith('    ') and current_resource is not None:
                project_name = resource_projet.strip()
                theoretical_charge = row.get('Charge Theorique', 0)
                
                current_projects.append(project_name)
                current_theoretical_charges.append(theoretical_charge)
        
        # Don't forget the last resource
        if current_resource is not None:
            theoretical_sum = sum([charge for charge in current_theoretical_charges if pd.notna(charge) and charge != 0])
            project_count = len(current_projects)
            result_data.append({
                'Ressource': current_resource,
                'Somme Charge Théorique': theoretical_sum,
                'Nombre de projets': project_count
            })
        
        if not result_data:
            return pd.DataFrame(columns=['Ressource', 'Somme Charge Théorique', 'Nombre de projets'])
        
        # Create DataFrame and sort by theoretical charge (highest to lowest)
        result_df = pd.DataFrame(result_data)
        result_df = result_df.sort_values('Somme Charge Théorique', ascending=False)
        
        return result_df