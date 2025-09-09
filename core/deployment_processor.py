import pandas as pd
import calendar
from config.raf_rules import get_raf


class DeploymentProcessor:
    """
    Processes deployment data and adds RAF calculations.
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
    def calculate_raf(df):
        """
        Calculate RAF values for each deployment based on connection level and project phase.

        Args:
            df (pandas.DataFrame): The deployments DataFrame with 'Niveau de connexion' and 'Phase du projet'

        Returns:
            pandas.DataFrame: DataFrame with added RAF column
        """
        result_df = df.copy()

        # Add RAF column with NaN values
        result_df['RAF'] = None

        # Calculate RAF for each row
        for idx, row in result_df.iterrows():
            connection_level = row.get('Niveau de connexion')
            project_phase = row.get('Phase du projet')

            if connection_level and project_phase:
                raf_value = get_raf(connection_level, project_phase)
                result_df.at[idx, 'RAF'] = raf_value

        return result_df

    @staticmethod
    def calculate_monthly_raf(df):
        """
        Calculate the sum of RAF values by month from 'Date de MEP'.

        Args:
            df (pandas.DataFrame): The deployments DataFrame with 'Date de MEP' and 'RAF' columns

        Returns:
            pandas.DataFrame: DataFrame with monthly RAF sums
        """
        # Create a copy of the dataframe
        monthly_df = df.copy()

        # Check if both required columns exist
        if 'Date de MEP' not in monthly_df.columns or 'RAF' not in monthly_df.columns:
            return pd.DataFrame(columns=['Month', 'Year', 'Month Name', 'Total RAF'])

        # Convert Date de MEP to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(monthly_df['Date de MEP']):
            # Try to convert while preserving original format
            monthly_df['Date de MEP'] = pd.to_datetime(monthly_df['Date de MEP'], errors='coerce')

        # Drop rows with missing dates or RAF values
        monthly_df = monthly_df.dropna(subset=['Date de MEP', 'RAF'])

        # Extract month and year
        monthly_df['Month'] = monthly_df['Date de MEP'].dt.month
        monthly_df['Year'] = monthly_df['Date de MEP'].dt.year

        # Add month name
        monthly_df['Month Name'] = monthly_df['Date de MEP'].dt.strftime('%B')

        # Group by month and year, sum the RAF values
        result = monthly_df.groupby(['Year', 'Month', 'Month Name'])['RAF'].sum().reset_index()

        # Rename the column for clarity
        result = result.rename(columns={'RAF': 'Total RAF'})

        # Sort by year and month
        result = result.sort_values(['Year', 'Month'])

        return result