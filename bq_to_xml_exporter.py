import logging
from pathlib import Path
from typing import Dict, List, Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import pandas as pd
from datetime import datetime

from config.cred.enviroment import Environment


class BigQueryToXMLExporter:
    """Exports BigQuery data to XML format grouped by mercant_id."""

    def __init__(
        self,
        bq_table: str,
        output_dir: str = "temp",
        pars_date_created_at: Optional[str] = None,
    ):
        """
        Initialize the BigQuery to XML exporter.

        Args:
            bq_table: BigQuery table name (format: project.dataset.table)
            output_dir: Directory to save XML files
            pars_date_created_at: Optional filter value for pars_date_created_at column
        """
        self._setup_logging()
        self._load_config()
        self.bq_table = bq_table
        self.pars_date_created_at = pars_date_created_at
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self) -> None:
        """Configure logging for the exporter."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _load_config(self) -> None:
        """Load BigQuery configuration from environment."""
        env = Environment()
        self.bq_client = env.bq_client
        if not self.bq_client:
            raise ValueError("BigQuery client not available from environment configuration")

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch data from BigQuery table.

        Returns:
            DataFrame with all columns from the table
        """
        self.logger.info(f"Fetching data from BigQuery table: {self.bq_table}")
        
        query = f"SELECT * FROM `{self.bq_table}`"
        # Optionally filter by pars_date_created_at if provided
        if self.pars_date_created_at:
            query += f" WHERE timestamp_trunc(pars_date_created_at, day) = '{self.pars_date_created_at}'"
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            self.logger.info(f"Successfully fetched {len(df)} records from BigQuery")
            return df
        except Exception as e:
            self.logger.error(f"Failed to fetch data from BigQuery: {str(e)}")
            raise

    def _escape_xml(self, text: str) -> str:
        """Escape XML special characters."""
        if pd.isna(text) or text is None:
            return ""
        text = str(text)
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;"))

    def _format_timestamp(self, ts) -> str:
        """Format timestamp to ISO format string."""
        if pd.isna(ts) or ts is None:
            return ""
        if isinstance(ts, datetime):
            return ts.isoformat()
        if isinstance(ts, str):
            return ts
        return str(ts)

    def _row_to_offer(self, row: pd.Series) -> Element:
        """
        Convert a DataFrame row to an XML offer element.

        Args:
            row: DataFrame row with product data

        Returns:
            XML Element representing an offer
        """
        # Use product_id or id as the offer id
        offer_id = str(row.get('product_id', row.get('id', '')))
        offer = Element('offer', id=offer_id)
        
        # Map BigQuery columns to XML elements
        mappings = {
            'title': 'name',
            'url': 'url',
            'price': 'price',
            'original_price': 'oldprice',
            'category_full_path': 'path',
            'sub_category': 'category',
            'url_picture': 'picture',
            'time_scrap': 'scrappedat',
            'measure': 'measure',
            'city': 'city',
        }
        
        for bq_col, xml_tag in mappings.items():
            value = row.get(bq_col)
            if value is not None and not pd.isna(value):
                elem = SubElement(offer, xml_tag)
                if xml_tag == 'scrappedat':
                    elem.text = self._format_timestamp(value)
                else:
                    elem.text = self._escape_xml(str(value))
            else:
                # Create empty element for required fields
                elem = SubElement(offer, xml_tag)
                elem.text = ""
        
        return offer

    def _dataframe_to_xml(self, df: pd.DataFrame) -> str:
        """
        Convert DataFrame to XML format matching the example.

        Args:
            df: DataFrame with product data

        Returns:
            XML string
        """
        # Create root element with current date attribute
        yml_catalog = Element('yml_catalog')
        # Example format: 2024-11-04 16:34
        yml_catalog.set('date', datetime.now().strftime('%Y-%m-%d %H:%M'))
        shop = SubElement(yml_catalog, 'shop')
        
        # Add name and company elements from the first row (all rows in group have same mercant_id/mercant_name)
        first_row = df.iloc[0]
        
        # Add <name> element from mercant_name
        name_elem = SubElement(shop, 'name')
        mercant_name = first_row.get('mercant_name', '')
        name_elem.text = self._escape_xml(str(mercant_name)) if not pd.isna(mercant_name) else ""
        
        # Add <company> element from mercant_id
        company_elem = SubElement(shop, 'company')
        mercant_id = first_row.get('mercant_id', '')
        company_elem.text = self._escape_xml(str(mercant_id)) if not pd.isna(mercant_id) else ""
        
        # Add empty categories element
        categories_elem = SubElement(shop, 'categories')
        categories_elem.text = ""
        
        offers = SubElement(shop, 'offers')
        
        # Add each row as an offer
        for _, row in df.iterrows():
            offer = self._row_to_offer(row)
            offers.append(offer)
        
        # Convert to string with proper formatting
        rough_string = tostring(yml_catalog, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        
        # Get formatted XML (without XML declaration for consistency with example)
        formatted = reparsed.documentElement.toprettyxml(indent="", encoding=None)
        
        # Remove the XML declaration line if present
        lines = formatted.split('\n')
        if lines and lines[0].startswith('<?xml'):
            lines = lines[1:]
        
        return '\n'.join(lines).strip()

    def export_to_xml(self, df: pd.DataFrame) -> List[str]:
        """
        Export DataFrame to XML files grouped by mercant_id.

        Args:
            df: DataFrame with product data

        Returns:
            List of created file paths
        """
        if df.empty:
            self.logger.warning("DataFrame is empty, no files to create")
            return []
        
        if 'mercant_id' not in df.columns:
            raise ValueError("DataFrame must contain 'mercant_id' column")
        
        created_files = []
        
        # Group by mercant_id
        for mercant_id, group_df in df.groupby('mercant_id'):
            if pd.isna(mercant_id) or mercant_id is None or mercant_id == "":
                self.logger.warning("Skipping row with empty mercant_id")
                continue
            
            # Convert to XML
            xml_content = self._dataframe_to_xml(group_df)
            
            # Create filename from mercant_id
            filename = f"{mercant_id}.xml"
            filepath = self.output_dir / filename
            
            # Write to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(xml_content)
            
            self.logger.info(f"Created XML file: {filepath} with {len(group_df)} records")
            created_files.append(str(filepath))
        
        return created_files

    def run(self) -> List[str]:
        """
        Execute the complete export process.

        Returns:
            List of created file paths
        """
        try:
            df = self.fetch_data()
            return self.export_to_xml(df)
        except Exception as e:
            self.logger.error(f"Export process failed: {str(e)}")
            raise


if __name__ == '__main__':
    # Set pars_date_created_at to filter data by this timestamp (BigQuery TIMESTAMP format).
    # Example: '2025-11-18'
    pars_date_created_at: Optional[str] = None  # TODO: set this value as needed
    pars_date_created_at: Optional[str] = '2026-02-02'

    exporter = BigQueryToXMLExporter(
        bq_table='organic-reef-315010.intermediate.int_parser_lavka_almaty',
        # bq_table='organic-reef-315010.intermediate.int_parser_lavka_astana',
        output_dir='temp',
        pars_date_created_at=pars_date_created_at,
    )
    
    files = exporter.run()
    print(f"Successfully created {len(files)} XML file(s):")
    for file in files:
        print(f"  - {file}")
