import json
import csv
from pathlib import Path
from typing import Dict, List, Optional

class TagManager:
    """
    Manages XBRL/iXBRL tags from multiple taxonomies (CSV) and a dictionary (JSON).
    """

    def __init__(self, tag_dict_path: str, taxonomy_dir: str):
        self.tag_dict_path = Path(tag_dict_path)
        self.taxonomy_dir = Path(taxonomy_dir)
        self.tag_dictionary: Dict[str, List[str]] = {}
        self.taxonomies: Dict[str, Dict[str, List[str]]] = {} # taxonomy_name -> {standardized_name -> [tags]}

        self._load_tag_dictionary()
        self._load_taxonomies()

    def _load_tag_dictionary(self):
        """Loads the tag definition JSON."""
        if self.tag_dict_path.exists():
            with open(self.tag_dict_path, 'r', encoding='utf-8') as f:
                self.tag_dictionary = json.load(f)
        else:
            print(f"Warning: Tag dictionary not found at {self.tag_dict_path}")

    def _load_taxonomies(self):
        """Loads all CSV taxonomy files from the directory."""
        # Map filenames to short taxonomy names if needed, or just use filename stem
        # Expected files: all.csv, char.csv, frs-102.csv, uk-gaap-full.csv
        for csv_file in self.taxonomy_dir.glob('*.csv'):
            taxonomy_name = csv_file.stem # e.g., 'frs-102', 'char'
            self.taxonomies[taxonomy_name] = {}
            
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tag = row.get('Element Name')
                        if tag:
                            # Just store existence for now
                            if taxonomy_name not in self.taxonomies:
                                self.taxonomies[taxonomy_name] = {}
                            if 'tags' not in self.taxonomies[taxonomy_name]:
                                self.taxonomies[taxonomy_name]['tags'] = []
                            self.taxonomies[taxonomy_name]['tags'].append(tag)
                            
            except Exception as e:
                print(f"Error loading taxonomy {csv_file}: {e}")

    def get_all_keys(self) -> List[str]:
        """Return all standardized column names defined in the dictionary."""
        return list(self.tag_dictionary.keys())

    def get_potential_tags(self, column_name: str) -> List[str]:
        """
        Returns a list of all potential XBRL tags for a given standardized column name.
        """
        # The dictionary now directly holds the list of tags
        return self.tag_dictionary.get(column_name, [])


