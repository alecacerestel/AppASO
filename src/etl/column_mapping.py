"""
Column mapping and standardization module.
Handles differences between Apple and Google data formats.
"""

from typing import Dict, List


class ColumnMapper:
    """
    Manages column name standardization across different data sources.
    """
    
    # File name patterns to identify each data source
    FILE_PATTERNS = {
        "keywords_apple": "APPLE motcles",
        "keywords_google": "GOOGLE motcles",
        "installs_apple": "Installs Apple",
        "installs_google": "Installs Google",
        "users_apple": "Utilisateurs connectés Apple",
        "users_google": "Utilisateurs connectés Google"
    }
    
    # Standard column names for final output
    STANDARD_COLUMNS = {
        "keywords": ["Date", "Rank_1", "Rank_2_3", "Rank_4_10", "Rank_11_30", "Rank_31_100", "Rank_100_Plus", "Platform", "Stage"],
        "installs": ["Date", "Installs", "Platform", "Stage"],
        "users": ["Date", "Active_Users", "Platform", "Notes", "Stage"]
    }
    
    @staticmethod
    def get_keywords_mapping(platform: str) -> Dict[str, str]:
        """
        Get column mapping for keywords data.
        Both Apple and Google use the same column names for keywords.
        
        Args:
            platform: "Apple" or "Google"
            
        Returns:
            Dictionary mapping original columns to standard columns
        """
        # Both platforms have identical structure for keywords
        return {
            "DateTime": "Date",
            "Rank 1": "Rank_1",
            "Rank 2 - 3": "Rank_2_3",
            "Rank 4 - 10": "Rank_4_10",
            "Rank 11-30": "Rank_11_30",
            "Rank 31-100": "Rank_31_100",
            "Rank 100+": "Rank_100_Plus"
        }
    
    @staticmethod
    def get_installs_mapping(platform: str) -> Dict[str, str]:
        """
        Get column mapping for installs data.
        
        Args:
            platform: "Apple" or "Google"
            
        Returns:
            Dictionary mapping original columns to standard columns
        """
        if platform == "Apple":
            return {
                "Date": "Date",
                "Installs Apple": "Installs"
            }
        else:  # Google
            return {
                "Date": "Date",
                "Installs Google Play": "Installs"
            }
    
    @staticmethod
    def get_users_mapping(platform: str) -> Dict[str, str]:
        """
        Get column mapping for active users data.
        Google and Apple have very different structures.
        
        Args:
            platform: "Apple" or "Google"
            
        Returns:
            Dictionary mapping original columns to standard columns
        """
        if platform == "Apple":
            # Apple file has unusual structure with metadata in first rows
            # Actual data starts after initial metadata rows
            return {
                "Nom": "Date",
                "Courses U : Magasin en ligne": "Active_Users"
            }
        else:  # Google
            return {
                "Date": "Date",
                # Long column name from Google Console
                "Utilisateurs actifs par mois (UAM) (Utilisateurs uniques, Par intervalle, Quotidiennes)\xa0: Tous les pays/r\xe9gions": "Active_Users",
                "Notes": "Notes"
            }
    
    @staticmethod
    def get_columns_to_keep(data_type: str) -> List[str]:
        """
        Get the list of standard columns to keep for each data type.
        
        Args:
            data_type: "keywords", "installs", or "users"
            
        Returns:
            List of column names to keep in final output
        """
        return ColumnMapper.STANDARD_COLUMNS.get(data_type, [])
