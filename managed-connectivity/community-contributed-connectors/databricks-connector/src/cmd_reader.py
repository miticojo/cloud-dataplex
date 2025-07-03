# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
from src.common.util import loadReferencedFile
from src.common.argument_validator import validateArguments

def read_args():
    parser = argparse.ArgumentParser()

    # Project arguments for basic generation of metadata entries
    parser.add_argument("--target_project_id", type=str, required=True,
                        help="Google Cloud Project ID metadata entries will be import into")
    parser.add_argument("--target_location_id", type=str, required=True,
                        help="Google Cloud region metadata will be imported into")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
                        help="Dataplex Entry Group ID to import metadata into")
    
    parser.add_argument("--jar", type=str, required=False, help="path to jar file")
    
    # Databricks specific arguments
    parser.add_argument("--server_hostname", type=str, required=True,help="Databricks server hostname to connect to")
    parser.add_argument("--http_path", type=str, required=True, help="Databricks compute resources URL")
    parser.add_argument("--access_token_secret", type=str, required=True, help="Google Cloud Secret Manager ID for personal access token")

    # Output destination arguments. Generate local only, or local + to Cloud Storage bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,help="Cloud Storage bucket for metadata import file. Do not include gs:// prefix")  
    parser.add_argument("--output_folder", type=str, required=False,help="Folder within bucket where generated metadata import file will be written. Specify folder name only")

    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,help="Minimum number of entries expected in metadata file, if less entries then file gets deleted. Safety mechanism for when using Full Entry Sync metadata jobs")
    
    parsed_args = parser.parse_known_args()[0]

    # Apply common argument validation checks first
    parsed_args = validateArguments(parsed_args)

    return vars(parsed_args)